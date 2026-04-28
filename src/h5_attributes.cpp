#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <algorithm>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {

//===--------------------------------------------------------------------===//
// h5_attributes - Read attributes from HDF5 datasets/groups
//===--------------------------------------------------------------------===//

struct AttributeInfo {
	std::string name;
	LogicalType type;
};

struct H5AttributesScanLayout {
	vector<std::optional<idx_t>> attribute_output_idxs;
	vector<idx_t> filename_output_idxs;
};

struct H5AttributesBindData : public TableFunctionData {
	vector<string> filenames;
	vector<AttributeInfo> attributes;
	std::string object_path;
	bool swmr = false;
	std::optional<idx_t> visible_filename_idx;
	bool had_glob = false;

	bool SupportStatementCache() const override {
		return !had_glob;
	}
};

struct H5AttributesGlobalState : public GlobalTableFunctionState {
	idx_t file_idx = 0;
	H5AttributesScanLayout output_layout;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct AttrIterData {
	vector<AttributeInfo> *attributes;
	bool error = false;
	std::string error_message;
};

static std::string NormalizeObjectPath(std::string object_path) {
	if (object_path.empty()) {
		return "/";
	}
	return object_path;
}

static herr_t attr_info_callback(hid_t location_id, const char *attr_name, const H5A_info_t *ainfo, void *op_data) {
	auto &iter_data = *reinterpret_cast<AttrIterData *>(op_data);
	if (iter_data.error) {
		return -1;
	}

	auto fail = [&](const std::string &message) {
		iter_data.error = true;
		iter_data.error_message = message;
		return -1;
	};

	auto &attributes = *iter_data.attributes;
	H5OpenedAttribute opened;
	LogicalType duckdb_type;
	try {
		opened = H5OpenAttribute(location_id, attr_name);
		duckdb_type = H5ResolveAttributeLogicalType(opened.type.get(), opened.space.get(), attr_name);
	} catch (const std::exception &ex) {
		return fail(H5NormalizeExceptionMessage(ex.what()));
	}

	AttributeInfo info;
	info.name = attr_name;
	info.type = duckdb_type;
	attributes.push_back(std::move(info));

	return 0; // Continue iteration
}

static bool H5AttributesOutputHasColumnName(const vector<string> &names, const string &column_name) {
	return std::any_of(names.begin(), names.end(),
	                   [&](const string &name) { return StringUtil::CIEquals(name, column_name); });
}

static virtual_column_map_t H5AttributesGetVirtualColumns(ClientContext &, optional_ptr<FunctionData> bind_data_p) {
	virtual_column_map_t result;
	if (bind_data_p && bind_data_p->Cast<H5AttributesBindData>().visible_filename_idx.has_value()) {
		return result;
	}
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR));
	return result;
}

static bool H5AttributesIsFilenameColumn(const H5AttributesBindData &bind_data, column_t column_id) {
	return column_id == MultiFileReader::COLUMN_IDENTIFIER_FILENAME ||
	       (bind_data.visible_filename_idx.has_value() && column_id == *bind_data.visible_filename_idx);
}

static H5AttributesScanLayout H5AttributesBuildOutputLayout(const H5AttributesBindData &bind_data,
                                                            const vector<column_t> &column_ids) {
	H5AttributesScanLayout result;
	result.attribute_output_idxs.resize(bind_data.attributes.size());
	for (idx_t output_idx = 0; output_idx < column_ids.size(); output_idx++) {
		auto column_id = column_ids[output_idx];
		if (H5AttributesIsFilenameColumn(bind_data, column_id)) {
			result.filename_output_idxs.push_back(output_idx);
			continue;
		}
		if (column_id >= bind_data.attributes.size()) {
			throw InternalException("h5_attributes column id %llu out of range", column_id);
		}
		result.attribute_output_idxs[column_id] = output_idx;
	}
	return result;
}

static vector<AttributeInfo> BindSingleH5AttributesFile(ClientContext &context, const string &filename,
                                                        const string &object_path, bool swmr) {
	ThrowIfInterrupted(context);

	vector<AttributeInfo> result;

	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	H5ErrorSuppressor suppress_errors;
	H5FileHandle file(&context, filename.c_str(), H5F_ACC_RDONLY, swmr);
	if (!file.is_valid()) {
		throw IOException(FormatRemoteFileError("Failed to open HDF5 file", filename));
	}

	H5ObjectHandle obj(file, object_path.c_str());
	if (!obj.is_valid()) {
		throw IOException(AppendRemoteError("Failed to open object: " + object_path, filename));
	}

	hsize_t idx = 0;
	AttrIterData iter_data;
	iter_data.attributes = &result;
	auto status = H5Aiterate2(obj, H5_INDEX_NAME, H5_ITER_NATIVE, &idx, attr_info_callback, &iter_data);
	if (status < 0) {
		if (iter_data.error) {
			throw IOException(AppendRemoteError(iter_data.error_message, filename));
		}
		throw IOException(AppendRemoteError("Failed to iterate attributes for: " + object_path, filename));
	}

	if (result.empty()) {
		throw IOException("Object has no attributes: " + object_path);
	}
	return result;
}

static bool H5AttributesSchemasMatch(const vector<AttributeInfo> &expected, const vector<AttributeInfo> &actual) {
	if (expected.size() != actual.size()) {
		return false;
	}
	for (idx_t i = 0; i < expected.size(); i++) {
		if (expected[i].name != actual[i].name || expected[i].type != actual[i].type) {
			return false;
		}
	}
	return true;
}

static unique_ptr<FunctionData> H5AttributesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	ThrowIfInterrupted(context);
	auto swmr = ResolveSwmrOption(context, input.named_parameters);
	auto filename_option = ResolveFilenameColumnOption(input.named_parameters);
	auto object_path = NormalizeObjectPath(GetRequiredStringArgument(input.inputs[1], "h5_attributes", "path"));
	auto expanded = H5ExpandFilePatterns(context, input.inputs[0], "h5_attributes");
	D_ASSERT(!expanded.filenames.empty());

	auto attributes = BindSingleH5AttributesFile(context, expanded.filenames[0], object_path, swmr);
	for (const auto &attr : attributes) {
		names.push_back(attr.name);
		return_types.push_back(attr.type);
	}
	if (filename_option.include) {
		if (H5AttributesOutputHasColumnName(names, filename_option.column_name)) {
			throw BinderException("Option filename adds column \"%s\", but that column name is already present in "
			                      "h5_attributes output",
			                      filename_option.column_name);
		}
		names.push_back(filename_option.column_name);
		return_types.push_back(LogicalType::VARCHAR);
	}

	auto result = make_uniq<H5AttributesBindData>();
	result->filenames = std::move(expanded.filenames);
	result->attributes = std::move(attributes);
	result->object_path = std::move(object_path);
	result->swmr = swmr;
	result->had_glob = expanded.had_glob;
	if (filename_option.include) {
		result->visible_filename_idx = names.size() - 1;
	}

	for (idx_t file_idx = 1; file_idx < result->filenames.size(); file_idx++) {
		auto file_attributes =
		    BindSingleH5AttributesFile(context, result->filenames[file_idx], result->object_path, swmr);
		if (!H5AttributesSchemasMatch(result->attributes, file_attributes)) {
			throw BinderException("h5_attributes matched file '%s' with an incompatible attribute schema",
			                      result->filenames[file_idx]);
		}
	}

	return result;
}

static unique_ptr<GlobalTableFunctionState> H5AttributesInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5AttributesBindData>();
	auto result = make_uniq<H5AttributesGlobalState>();
	result->output_layout = H5AttributesBuildOutputLayout(bind_data, input.column_ids);
	return std::move(result);
}

static void H5AttributesPopulateFilenameColumns(const string &filename, const H5AttributesScanLayout &layout,
                                                DataChunk &output, idx_t row_idx) {
	for (auto output_idx : layout.filename_output_idxs) {
		output.data[output_idx].SetValue(row_idx, Value(filename));
	}
}

static Value H5AttributesReadAttributeValue(H5ObjectHandle &obj, const AttributeInfo &attr_info,
                                            const string &filename) {
	try {
		auto opened = H5OpenAttribute(obj, attr_info.name);
		return H5ReadAttributeValue(opened.attr, opened.type.get(), opened.space.get(), attr_info.type, attr_info.name);
	} catch (const std::exception &ex) {
		throw IOException(AppendRemoteError(H5NormalizeExceptionMessage(ex.what()), filename));
	}
}

static void H5AttributesWriteFileRow(ClientContext &context, const H5AttributesBindData &bind_data,
                                     const string &filename, const H5AttributesScanLayout &layout, DataChunk &output,
                                     idx_t row_idx) {
	ThrowIfInterrupted(context);

	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	H5ErrorSuppressor suppress_errors;
	H5FileHandle file(&context, filename.c_str(), H5F_ACC_RDONLY, bind_data.swmr);
	if (!file.is_valid()) {
		throw IOException(FormatRemoteFileError("Failed to open HDF5 file", filename));
	}

	H5ObjectHandle obj(file, bind_data.object_path.c_str());
	if (!obj.is_valid()) {
		throw IOException(AppendRemoteError("Failed to open object: " + bind_data.object_path, filename));
	}

	for (idx_t attr_idx = 0; attr_idx < bind_data.attributes.size(); attr_idx++) {
		auto value = H5AttributesReadAttributeValue(obj, bind_data.attributes[attr_idx], filename);
		auto output_idx = layout.attribute_output_idxs[attr_idx];
		if (output_idx.has_value()) {
			output.data[*output_idx].SetValue(row_idx, value);
		}
	}
}

static void H5AttributesScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &gstate = input.global_state->Cast<H5AttributesGlobalState>();
	auto &bind_data = input.bind_data->Cast<H5AttributesBindData>();

	idx_t row_idx = 0;
	while (gstate.file_idx < bind_data.filenames.size() && row_idx < STANDARD_VECTOR_SIZE) {
		auto &filename = bind_data.filenames[gstate.file_idx];
		H5AttributesWriteFileRow(context, bind_data, filename, gstate.output_layout, output, row_idx);
		H5AttributesPopulateFilenameColumns(filename, gstate.output_layout, output, row_idx);
		gstate.file_idx++;
		row_idx++;
	}
	output.SetCardinality(row_idx);
}

void RegisterH5AttributesFunction(ExtensionLoader &loader) {
	TableFunction h5_attributes("h5_attributes", {LogicalType::VARCHAR, LogicalType::VARCHAR}, H5AttributesScan,
	                            H5AttributesBind, H5AttributesInit);
	h5_attributes.name = "h5_attributes";
	h5_attributes.named_parameters["filename"] = LogicalType::ANY;
	h5_attributes.named_parameters["swmr"] = LogicalType::BOOLEAN;
	// Projection pushdown is enabled so DuckDB can bind hidden virtual columns.
	// h5_attributes still intentionally reads every attribute for each emitted row.
	h5_attributes.projection_pushdown = true;
	h5_attributes.get_virtual_columns = H5AttributesGetVirtualColumns;

	loader.RegisterFunction(MultiFileReader::CreateFunctionSet(std::move(h5_attributes)));
}

} // namespace duckdb
