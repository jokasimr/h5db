#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "h5_tree_shared.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include <algorithm>
#include <optional>
#include <string>
#include <unordered_map>
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

struct H5AttributesScalarBindData : public FunctionData {
	bool swmr = false;

	explicit H5AttributesScalarBindData(bool swmr_p) : swmr(swmr_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<H5AttributesScalarBindData>(swmr);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<H5AttributesScalarBindData>();
		return swmr == other.swmr;
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
		throw IOException(FormatHDF5ObjectError("Failed to open object", filename, object_path));
	}

	hsize_t idx = 0;
	AttrIterData iter_data;
	iter_data.attributes = &result;
	auto status = H5Aiterate2(obj, H5_INDEX_NAME, H5_ITER_NATIVE, &idx, attr_info_callback, &iter_data);
	if (status < 0) {
		if (iter_data.error) {
			throw IOException(FormatHDF5ObjectContextError(iter_data.error_message, filename, object_path));
		}
		throw IOException(FormatHDF5ObjectError("Failed to iterate attributes for object", filename, object_path));
	}

	if (result.empty()) {
		throw IOException(FormatHDF5ObjectError("Object has no attributes", filename, object_path));
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

static Value H5AttributesReadAttributeValue(H5ObjectHandle &obj, const AttributeInfo &attr_info, const string &filename,
                                            const string &object_path) {
	try {
		auto opened = H5OpenAttribute(obj, attr_info.name);
		return H5ReadAttributeValue(opened.attr, opened.type.get(), opened.space.get(), attr_info.type, attr_info.name);
	} catch (const std::exception &ex) {
		throw IOException(FormatHDF5ObjectContextError(H5NormalizeExceptionMessage(ex.what()), filename, object_path));
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
		throw IOException(FormatHDF5ObjectError("Failed to open object", filename, bind_data.object_path));
	}

	for (idx_t attr_idx = 0; attr_idx < bind_data.attributes.size(); attr_idx++) {
		auto value =
		    H5AttributesReadAttributeValue(obj, bind_data.attributes[attr_idx], filename, bind_data.object_path);
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

class H5AttributesScalarFileReader {
public:
	H5AttributesScalarFileReader(ClientContext &context_p, string filename_p, bool swmr_p)
	    : context(context_p), filename(std::move(filename_p)) {
		H5ErrorSuppressor suppress_errors;
		file = H5FileHandle(&context, filename.c_str(), H5F_ACC_RDONLY, swmr_p);
		if (!file.is_valid()) {
			throw IOException(FormatRemoteFileError("Failed to open HDF5 file", filename));
		}
	}

	Value ReadObjectAttributes(const string_t &path_value) {
		ThrowIfInterrupted(context);
		auto object_path = NormalizeObjectPath(path_value.GetString());

		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
		H5ErrorSuppressor suppress_errors;
		H5ObjectHandle obj(file, object_path.c_str());
		if (!obj.is_valid()) {
			throw IOException(FormatHDF5ObjectError("Failed to open object", filename, object_path));
		}

		try {
			return H5ReadAllAttributesMapValue(obj);
		} catch (const std::exception &ex) {
			throw IOException(FormatHDF5ObjectContextError(H5NormalizeExceptionMessage(ex.what()), filename,
			                                               object_path));
		}
	}

private:
	ClientContext &context;
	string filename;
	H5FileHandle file;
};

struct H5AttributesScalarFileRows {
	string filename;
	vector<idx_t> row_idxs;
};

static unique_ptr<FunctionData> H5AttributesScalarBind(ClientContext &context, ScalarFunction &,
                                                       vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw InvalidInputException("scalar h5_attributes requires exactly 2 arguments: filename and object path");
	}
	auto swmr = ResolveSwmrOption(context, named_parameter_map_t {});
	return make_uniq<H5AttributesScalarBindData>(swmr);
}

static void H5AttributesScalarWriteFileRows(ClientContext &context, const H5AttributesScalarFileRows &file_rows,
                                            const UnifiedVectorFormat &path_data, const string_t *path_ptr,
                                            Vector &result, const H5AttributesScalarBindData &bind_data) {
	if (file_rows.row_idxs.empty()) {
		return;
	}
	ThrowIfInterrupted(context);
	H5AttributesScalarFileReader reader(context, file_rows.filename, bind_data.swmr);
	for (auto row_idx : file_rows.row_idxs) {
		auto path_idx = path_data.sel->get_index(row_idx);
		result.SetValue(row_idx, reader.ReadObjectAttributes(path_ptr[path_idx]));
	}
}

static void H5AttributesScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<H5AttributesScalarBindData>();
	if (args.size() == 0) {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		return;
	}

	auto &filename_vec = args.data[0];
	auto &path_vec = args.data[1];
	UnifiedVectorFormat filename_data;
	UnifiedVectorFormat path_data;
	filename_vec.ToUnifiedFormat(args.size(), filename_data);
	path_vec.ToUnifiedFormat(args.size(), path_data);
	auto filename_ptr = UnifiedVectorFormat::GetData<string_t>(filename_data);
	auto path_ptr = UnifiedVectorFormat::GetData<string_t>(path_data);
	auto &context = state.GetContext();
	auto constant_filename = filename_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto constant_path = path_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;

	if (constant_filename && constant_path) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto filename_idx = filename_data.sel->get_index(0);
		auto path_idx = path_data.sel->get_index(0);
		if (!filename_data.validity.RowIsValid(filename_idx) || !path_data.validity.RowIsValid(path_idx)) {
			ConstantVector::SetNull(result, true);
			return;
		}

		auto filename = filename_ptr[filename_idx].GetString();
		H5AttributesScalarFileReader reader(context, filename, bind_data.swmr);
		result.SetValue(0, reader.ReadObjectAttributes(path_ptr[path_idx]));
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &validity = FlatVector::Validity(result);

	if (constant_filename) {
		auto filename_idx = filename_data.sel->get_index(0);
		if (!filename_data.validity.RowIsValid(filename_idx)) {
			for (idx_t i = 0; i < args.size(); i++) {
				validity.SetInvalid(i);
			}
			return;
		}

		H5AttributesScalarFileRows file_rows;
		file_rows.filename = filename_ptr[filename_idx].GetString();
		file_rows.row_idxs.reserve(args.size());
		for (idx_t i = 0; i < args.size(); i++) {
			auto path_idx = path_data.sel->get_index(i);
			if (!path_data.validity.RowIsValid(path_idx)) {
				validity.SetInvalid(i);
				continue;
			}
			validity.SetValid(i);
			file_rows.row_idxs.push_back(i);
		}

		H5AttributesScalarWriteFileRows(context, file_rows, path_data, path_ptr, result, bind_data);
		return;
	}

	std::unordered_map<string, idx_t> file_group_lookup;
	vector<H5AttributesScalarFileRows> file_groups;
	file_group_lookup.reserve(args.size());
	file_groups.reserve(args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		auto filename_idx = filename_data.sel->get_index(i);
		auto path_idx = path_data.sel->get_index(i);
		if (!filename_data.validity.RowIsValid(filename_idx) || !path_data.validity.RowIsValid(path_idx)) {
			validity.SetInvalid(i);
			continue;
		}
		validity.SetValid(i);
		auto filename = filename_ptr[filename_idx].GetString();
		auto inserted = file_group_lookup.emplace(filename, file_groups.size());
		if (inserted.second) {
			file_groups.emplace_back();
			file_groups.back().filename = std::move(filename);
		}
		file_groups[inserted.first->second].row_idxs.push_back(i);
	}

	for (const auto &file_group : file_groups) {
		H5AttributesScalarWriteFileRows(context, file_group, path_data, path_ptr, result, bind_data);
	}
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

	auto h5_attributes_set = MultiFileReader::CreateFunctionSet(std::move(h5_attributes));
	CreateTableFunctionInfo info(std::move(h5_attributes_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(H5FunctionDescription(
	    {LogicalType::ANY, LogicalType::VARCHAR}, {"filename_or_filenames", "object_path", "swmr", "filename"},
	    "Reads all attributes from an HDF5 object or file root.", {"FROM h5_attributes('data.h5', '/measurements')"}));
	loader.RegisterFunction(std::move(info));

	ScalarFunction h5_attributes_scalar(
	    "h5_attributes", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARIANT()), H5AttributesScalarFunction,
	    H5AttributesScalarBind);
	h5_attributes_scalar.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	CreateScalarFunctionInfo scalar_info(std::move(h5_attributes_scalar));
	scalar_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	scalar_info.descriptions.push_back(H5FunctionDescription(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR}, {"filename", "object_path"},
	    "Reads all attributes from one HDF5 object as a MAP.", {"SELECT h5_attributes('data.h5', '/measurements')"}));
	loader.RegisterFunction(std::move(scalar_info));
}

} // namespace duckdb
