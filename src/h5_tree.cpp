#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_tree_shared.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include <memory>
#include <mutex>
#include <vector>

namespace duckdb {

struct H5TreeBindData : public TableFunctionData {
	vector<string> filenames;
	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	bool swmr = false;
	std::optional<idx_t> visible_filename_idx;
	bool had_glob = false;

	bool SupportStatementCache() const override {
		return !had_glob;
	}
};

struct H5TreeOutputColumn {
	idx_t output_idx;
	column_t column_id;
};

struct H5TreeScanLayout {
	vector<H5TreeOutputColumn> projected_columns;
	vector<idx_t> filename_output_idxs;
	vector<idx_t> empty_output_idxs;
	std::optional<idx_t> shape_output_idx;
	H5TreeReadOptions read_options;
};

class H5TreeScanner;

struct H5TreeGlobalState : public GlobalTableFunctionState {
	unique_ptr<H5TreeScanner> scanner;
	idx_t file_idx = 0;
	H5TreeScanLayout output_layout;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct H5TreeAncestryNode {
	H5TreeObjectIdentity identity;
	std::shared_ptr<const H5TreeAncestryNode> parent;
};

struct H5TreeGroupFrame {
	std::string path;
	std::shared_ptr<const H5TreeAncestryNode> ancestry;
	hsize_t next_idx = 0;
};

struct H5TreeIterationContext {
	ClientContext &context;
	H5TreeFileReader &reader;
	const H5TreeGroupFrame &frame;
	vector<H5TreeRow> &rows;
	vector<H5TreeGroupFrame> child_frames;
	bool interrupted = false;
	std::optional<std::string> error_message;
};

static bool H5TreeOutputHasColumnName(const vector<string> &names, const string &column_name) {
	return std::any_of(names.begin(), names.end(),
	                   [&](const string &name) { return StringUtil::CIEquals(name, column_name); });
}

static virtual_column_map_t H5TreeGetFilenameVirtualColumns(ClientContext &, optional_ptr<FunctionData> bind_data_p) {
	virtual_column_map_t result;
	if (!bind_data_p || !bind_data_p->Cast<H5TreeBindData>().visible_filename_idx.has_value()) {
		result.insert(
		    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR)));
	}
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
	return result;
}

static bool H5TreeIsFilenameColumn(const H5TreeBindData &bind_data, column_t column_id) {
	return column_id == MultiFileReader::COLUMN_IDENTIFIER_FILENAME ||
	       (bind_data.visible_filename_idx.has_value() && column_id == *bind_data.visible_filename_idx);
}

static void H5TreePopulateFilenameColumns(const string &filename, const vector<idx_t> &filename_output_idxs,
                                          DataChunk &output) {
	if (output.size() == 0) {
		return;
	}
	for (auto output_idx : filename_output_idxs) {
		auto &vector = output.data[output_idx];
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<string_t>(vector)[0] = StringVector::AddString(vector, filename);
	}
}

static void H5TreePopulateEmptyColumns(const vector<idx_t> &empty_output_idxs, DataChunk &output) {
	if (output.size() == 0) {
		return;
	}
	for (auto output_idx : empty_output_idxs) {
		auto &vector = output.data[output_idx];
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(vector, true);
	}
}

static H5TreeScanLayout H5TreeBuildOutputLayout(const H5TreeBindData &bind_data, const vector<column_t> &column_ids) {
	H5TreeScanLayout result;
	for (idx_t output_idx = 0; output_idx < column_ids.size(); output_idx++) {
		auto column_id = column_ids[output_idx];
		if (column_id == COLUMN_IDENTIFIER_EMPTY) {
			result.empty_output_idxs.push_back(output_idx);
			continue;
		}
		if (H5TreeIsFilenameColumn(bind_data, column_id)) {
			result.filename_output_idxs.push_back(output_idx);
			continue;
		}
		if (column_id >= 4 + bind_data.projected_attributes.size()) {
			throw InternalException("h5_tree column id %llu out of range", column_id);
		}
		switch (column_id) {
		case 0:
			break;
		case 1:
			result.read_options.read_type = true;
			break;
		case 2:
			result.read_options.read_dtype = true;
			break;
		case 3:
			result.read_options.read_shape = true;
			result.shape_output_idx = output_idx;
			break;
		default:
			result.read_options.projected_attribute_ids.push_back(column_id - 4);
			break;
		}
		result.projected_columns.push_back({output_idx, column_id});
	}
	return result;
}

static bool H5TreeAncestryContains(H5TreeFileReader &reader, const std::shared_ptr<const H5TreeAncestryNode> &ancestry,
                                   const H5TreeObjectIdentity &identity) {
	for (auto node = ancestry; node; node = node->parent) {
		if (reader.SameObject(node->identity, identity)) {
			return true;
		}
	}
	return false;
}

static std::string H5TreeJoinPath(const std::string &parent, const char *name) {
	if (parent == "/") {
		return "/" + std::string(name);
	}
	return parent + "/" + name;
}

class H5TreeScanner {
public:
	H5TreeScanner(ClientContext &context_p, const string &filename_p, bool swmr_p,
	              const vector<H5TreeProjectedAttributeSpec> &projected_attributes_p, H5TreeReadOptions read_options)
	    : context(context_p), reader(context_p, filename_p, swmr_p, projected_attributes_p, std::move(read_options)) {
		auto root_ancestry =
		    std::make_shared<H5TreeAncestryNode>(H5TreeAncestryNode {reader.GetRootIdentity(), nullptr});
		group_stack.push_back(H5TreeGroupFrame {"/", std::move(root_ancestry)});
	}

	void ReadRows(vector<H5TreeRow> &rows) {
		rows.clear();
		rows.reserve(STANDARD_VECTOR_SIZE);
		ThrowIfInterrupted(context);

		if (!root_emitted) {
			ProduceRootRow(rows);
			root_emitted = true;
		}

		while (rows.size() < STANDARD_VECTOR_SIZE && !group_stack.empty()) {
			ThrowIfInterrupted(context);
			vector<H5TreeGroupFrame> child_frames;
			auto complete = ReadTopGroupSlice(rows, child_frames);
			if (complete) {
				group_stack.pop_back();
			}
			for (auto child = child_frames.rbegin(); child != child_frames.rend(); ++child) {
				group_stack.push_back(std::move(*child));
			}
		}
	}

private:
	ClientContext &context;
	H5TreeFileReader reader;
	vector<H5TreeGroupFrame> group_stack;
	bool root_emitted = false;

	void ProduceRootRow(vector<H5TreeRow> &rows) {
		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
		H5TreeRow row;
		row.path = "/";
		if (reader.ReadsType()) {
			row.type = H5TreeTypeName(H5TreeEntryType::GROUP);
		}
		reader.InitializeProjectedValues(row);
		reader.PopulateRowMetadataAndAttributes(row, H5TreeEntryType::GROUP, "/", -1, nullptr);
		rows.push_back(std::move(row));
	}

	bool ReadTopGroupSlice(vector<H5TreeRow> &rows, vector<H5TreeGroupFrame> &child_frames) {
		D_ASSERT(rows.size() < STANDARD_VECTOR_SIZE);
		auto &frame = group_stack.back();
		H5TreeIterationContext iter_data {context, reader, frame, rows};

		herr_t status;
		bool reached_end = false;
		{
			std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
			H5ErrorSuppressor suppress;
			status = H5Literate_by_name2(reader.GetFileHandle(), frame.path.c_str(), H5_INDEX_NAME, H5_ITER_NATIVE,
			                             &frame.next_idx, IterateCallback, &iter_data, H5P_DEFAULT);
			if (status > 0 && !iter_data.interrupted) {
				// A positive callback result only says that iteration stopped. It does not distinguish between a
				// full output vector with links remaining and one whose last row was the group's final link.
				// Resuming at idx == nlinks is an HDF5 error, so resolve that ambiguity before retaining the frame.
				H5G_info_t group_info;
				if (H5Gget_info_by_name(reader.GetFileHandle(), frame.path.c_str(), &group_info, H5P_DEFAULT) < 0) {
					iter_data.error_message = FormatHDF5ObjectError("Failed to inspect group during tree traversal",
					                                                reader.GetFilename(), frame.path);
					status = -1;
				} else {
					reached_end = frame.next_idx >= group_info.nlinks;
				}
			}
		}

		if (iter_data.interrupted) {
			ThrowIfInterrupted(context);
		}
		if (status < 0) {
			if (iter_data.error_message) {
				throw IOException(AppendRemoteError(*iter_data.error_message, reader.GetFilename()));
			}
			throw IOException(FormatHDF5ObjectError("Failed to traverse HDF5 group", reader.GetFilename(), frame.path));
		}
		child_frames = std::move(iter_data.child_frames);
		return status == 0 || reached_end;
	}

	static herr_t IterateCallback(hid_t parent_loc, const char *name, const H5L_info2_t *info, void *op_data) {
		auto &data = *reinterpret_cast<H5TreeIterationContext *>(op_data);
		try {
			if (IsInterrupted(data.context)) {
				data.interrupted = true;
				return 1;
			}

			auto path = H5TreeJoinPath(data.frame.path, name);
			auto resolved = data.reader.ResolveEntry(*info, parent_loc, name);
			H5TreeRow row;
			row.path = path;
			if (data.reader.ReadsType()) {
				row.type = H5TreeTypeName(resolved.type_kind);
			}
			data.reader.InitializeProjectedValues(row);
			if (resolved.identity) {
				data.reader.PopulateRowMetadataAndAttributes(row, resolved.type_kind, path, parent_loc, name);
			}
			data.rows.push_back(std::move(row));

			if (resolved.type_kind == H5TreeEntryType::GROUP && resolved.identity &&
			    !H5TreeAncestryContains(data.reader, data.frame.ancestry, *resolved.identity)) {
				auto ancestry =
				    std::make_shared<H5TreeAncestryNode>(H5TreeAncestryNode {*resolved.identity, data.frame.ancestry});
				data.child_frames.push_back(H5TreeGroupFrame {std::move(path), std::move(ancestry)});
			}

			return data.rows.size() >= STANDARD_VECTOR_SIZE ? 1 : 0;
		} catch (const std::exception &ex) {
			data.error_message = H5NormalizeExceptionMessage(ex.what());
			return -1;
		}
	}
};

static unique_ptr<FunctionData> H5TreeBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5TreeBindData>();
	result->swmr = ResolveSwmrOption(context, input.named_parameters);
	auto expanded = H5ExpandFilePatterns(context, input.inputs[0], "h5_tree");
	result->filenames = std::move(expanded.filenames);
	result->had_glob = expanded.had_glob;

	names = {"path", "type", "dtype", "shape"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::UBIGINT)};
	H5TreeBindProjectedAttributes("h5_tree", input.inputs, 1, names, return_types, result->projected_attributes);
	auto filename_option = ResolveFilenameColumnOption(input.named_parameters);
	if (filename_option.include) {
		if (H5TreeOutputHasColumnName(names, filename_option.column_name)) {
			throw BinderException("Option filename adds column \"%s\", but that column name is already present in "
			                      "h5_tree output",
			                      filename_option.column_name);
		}
		result->visible_filename_idx = names.size();
		names.push_back(filename_option.column_name);
		return_types.push_back(LogicalType::VARCHAR);
	}
	return std::move(result);
}

static void H5TreePushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                        vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<H5TreeBindData>();
	H5ApplyFilenameFilterPushdown(context, get, bind_data.visible_filename_idx, bind_data.filenames, filters);
}

static void H5TreeOpenFileScanner(ClientContext &context, const H5TreeBindData &bind_data, H5TreeGlobalState &state,
                                  idx_t file_idx) {
	D_ASSERT(file_idx < bind_data.filenames.size());
	state.file_idx = file_idx;
	state.scanner = make_uniq<H5TreeScanner>(context, bind_data.filenames[file_idx], bind_data.swmr,
	                                         bind_data.projected_attributes, state.output_layout.read_options);
}

static unique_ptr<GlobalTableFunctionState> H5TreeInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5TreeBindData>();
	auto result = make_uniq<H5TreeGlobalState>();
	result->output_layout = H5TreeBuildOutputLayout(bind_data, input.column_ids);
	if (!bind_data.filenames.empty()) {
		H5TreeOpenFileScanner(context, bind_data, *result, 0);
	}
	return std::move(result);
}

static void H5TreeScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &bind_data = data.bind_data->Cast<H5TreeBindData>();
	auto &gstate = data.global_state->Cast<H5TreeGlobalState>();
	vector<H5TreeRow> rows;
	if (!gstate.scanner) {
		D_ASSERT(bind_data.filenames.empty());
		output.SetCardinality(0);
		return;
	}
	while (true) {
		gstate.scanner->ReadRows(rows);
		if (!rows.empty()) {
			break;
		}
		auto next_file_idx = gstate.file_idx + 1;
		if (next_file_idx >= bind_data.filenames.size()) {
			output.SetCardinality(0);
			return;
		}
		H5TreeOpenFileScanner(context, bind_data, gstate, next_file_idx);
	}

	output.SetCardinality(rows.size());

	idx_t total_shape_elems = 0;
	idx_t shape_offset = 0;
	uint64_t *shape_data = nullptr;
	auto shape_output_idx = gstate.output_layout.shape_output_idx;
	if (shape_output_idx.has_value()) {
		for (auto &row : rows) {
			if (row.shape) {
				total_shape_elems += row.shape->size();
			}
		}
		auto &shape_vector = output.data[*shape_output_idx];
		ListVector::Reserve(shape_vector, total_shape_elems);
		auto &child = ListVector::GetEntry(shape_vector);
		shape_data = FlatVector::GetData<uint64_t>(child);
	}

	for (idx_t row_idx = 0; row_idx < rows.size(); row_idx++) {
		for (const auto &output_column : gstate.output_layout.projected_columns) {
			H5TreeWriteProjectedValue(rows[row_idx], bind_data.projected_attributes, output_column.column_id,
			                          output.data[output_column.output_idx], row_idx, shape_offset, shape_data);
		}
	}
	if (shape_output_idx.has_value()) {
		ListVector::SetListSize(output.data[*shape_output_idx], shape_offset);
	}
	H5TreePopulateFilenameColumns(bind_data.filenames[gstate.file_idx], gstate.output_layout.filename_output_idxs,
	                              output);
	H5TreePopulateEmptyColumns(gstate.output_layout.empty_output_idxs, output);
}

void RegisterH5TreeFunction(ExtensionLoader &loader) {
	TableFunction function("h5_tree", {LogicalType::VARCHAR}, H5TreeScan, H5TreeBind, H5TreeInit);
	function.varargs = LogicalType::ANY;
	function.named_parameters["filename"] = LogicalType::ANY;
	function.named_parameters["swmr"] = LogicalType::BOOLEAN;
	function.projection_pushdown = true;
	function.pushdown_complex_filter = H5TreePushdownComplexFilter;
	function.get_virtual_columns = H5TreeGetFilenameVirtualColumns;
	auto function_set = MultiFileReader::CreateFunctionSet(std::move(function));
	CreateTableFunctionInfo info(std::move(function_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(H5FunctionDescription({LogicalType::ANY}, {"filename_or_filenames", "swmr", "filename"},
	                                                  "Recursively lists entries in an HDF5 file.",
	                                                  {"FROM h5_tree('data.h5')"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
