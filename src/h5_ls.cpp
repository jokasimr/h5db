#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_tree_shared.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include <algorithm>
#include <unordered_map>
#include <vector>

namespace duckdb {

struct H5LsBindData : public TableFunctionData {
	vector<string> filenames;
	std::string group_path;
	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	bool swmr = false;
	std::optional<idx_t> visible_filename_idx;
	bool had_glob = false;

	bool SupportStatementCache() const override {
		return !had_glob;
	}
};

struct H5LsOutputColumn {
	idx_t output_idx;
	column_t column_id;
};

struct H5LsScanLayout {
	vector<H5LsOutputColumn> projected_columns;
	vector<idx_t> filename_output_idxs;
	std::optional<idx_t> shape_output_idx;
};

class H5LsFileScanner {
public:
	H5LsFileScanner(ClientContext &context, const string &filename, const string &group_path_p, bool swmr,
	                const vector<H5TreeProjectedAttributeSpec> &projected_attributes)
	    : group_path(group_path_p), reader(context, filename, swmr, projected_attributes) {
		H5TreeValidateListGroup(reader, group_path);
	}

	bool ReadRows(ClientContext &context, std::vector<H5TreeNamedRow> &rows) {
		ThrowIfInterrupted(context);
		if (exhausted) {
			rows.clear();
			return true;
		}
		exhausted = H5TreeListEntriesBatch(reader, group_path, next_idx, STANDARD_VECTOR_SIZE, rows);
		return exhausted;
	}

private:
	std::string group_path;
	H5TreeFileReader reader;
	hsize_t next_idx = 0;
	bool exhausted = false;
};

struct H5LsGlobalState : public GlobalTableFunctionState {
	unique_ptr<H5LsFileScanner> scanner;
	std::vector<H5TreeNamedRow> batch_rows;
	idx_t file_idx = 0;
	H5LsScanLayout output_layout;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct H5LsScalarBindData : public FunctionData {
	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	bool swmr = false;

	H5LsScalarBindData(vector<H5TreeProjectedAttributeSpec> projected_attributes_p, bool swmr_p)
	    : projected_attributes(std::move(projected_attributes_p)), swmr(swmr_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<H5LsScalarBindData>(projected_attributes, swmr);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<H5LsScalarBindData>();
		if (swmr != other.swmr || projected_attributes.size() != other.projected_attributes.size()) {
			return false;
		}
		for (idx_t i = 0; i < projected_attributes.size(); i++) {
			const auto &lhs = projected_attributes[i];
			const auto &rhs = other.projected_attributes[i];
			if (lhs.all_attributes != rhs.all_attributes || lhs.attribute_name != rhs.attribute_name ||
			    lhs.output_column_name != rhs.output_column_name || lhs.output_type != rhs.output_type ||
			    lhs.default_value.ToString() != rhs.default_value.ToString()) {
				return false;
			}
		}
		return true;
	}
};

static bool H5LsOutputHasColumnName(const vector<string> &names, const string &column_name) {
	return std::any_of(names.begin(), names.end(),
	                   [&](const string &name) { return StringUtil::CIEquals(name, column_name); });
}

static virtual_column_map_t H5GetFilenameVirtualColumns(ClientContext &, optional_ptr<FunctionData> bind_data_p) {
	virtual_column_map_t result;
	if (bind_data_p && bind_data_p->Cast<H5LsBindData>().visible_filename_idx.has_value()) {
		return result;
	}
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR));
	return result;
}

static bool H5LsIsFilenameColumn(const H5LsBindData &bind_data, column_t column_id) {
	return column_id == MultiFileReader::COLUMN_IDENTIFIER_FILENAME ||
	       (bind_data.visible_filename_idx.has_value() && column_id == *bind_data.visible_filename_idx);
}

static void H5LsPopulateFilenameColumns(const string &filename, const vector<idx_t> &filename_output_idxs,
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

static H5LsScanLayout H5LsBuildOutputLayout(const H5LsBindData &bind_data, const vector<column_t> &column_ids) {
	H5LsScanLayout result;
	for (idx_t output_idx = 0; output_idx < column_ids.size(); output_idx++) {
		auto column_id = column_ids[output_idx];
		if (H5LsIsFilenameColumn(bind_data, column_id)) {
			result.filename_output_idxs.push_back(output_idx);
			continue;
		}
		if (column_id == 3) {
			result.shape_output_idx = output_idx;
		}
		result.projected_columns.push_back({output_idx, column_id});
	}
	return result;
}

static void H5LsGetReturnSchema(const vector<H5TreeProjectedAttributeSpec> &projected_attributes, vector<string> &names,
                                vector<LogicalType> &return_types) {
	names = {"path", "type", "dtype", "shape"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::UBIGINT)};
	for (const auto &spec : projected_attributes) {
		names.push_back(spec.output_column_name);
		return_types.push_back(spec.output_type);
	}
}

static void H5LsValidateUniqueFieldNames(const vector<string> &names) {
	case_insensitive_set_t seen;
	seen.reserve(names.size());
	for (const auto &name : names) {
		if (!seen.insert(name).second) {
			throw BinderException("table \"h5_ls\" has duplicate column name \"%s\"", name);
		}
	}
}

static Value H5LsRowToStructValue(const H5TreeNamedRow &named_row, const vector<string> &names,
                                  const vector<LogicalType> &return_types,
                                  const vector<H5TreeProjectedAttributeSpec> &projected_attributes) {
	child_list_t<Value> child_values;
	child_values.emplace_back(names[0], Value(named_row.row.path));
	child_values.emplace_back(names[1], named_row.row.type ? Value(*named_row.row.type) : Value(return_types[1]));
	child_values.emplace_back(names[2], named_row.row.dtype ? Value(*named_row.row.dtype) : Value(return_types[2]));

	if (named_row.row.has_shape) {
		vector<Value> dims;
		dims.reserve(named_row.row.shape.size());
		for (auto dim : named_row.row.shape) {
			dims.emplace_back(Value::UBIGINT(static_cast<uint64_t>(dim)));
		}
		child_values.emplace_back(names[3], Value::LIST(LogicalType::UBIGINT, std::move(dims)));
	} else {
		child_values.emplace_back(names[3], Value(return_types[3]));
	}

	for (idx_t i = 0; i < projected_attributes.size(); i++) {
		const auto &projected = named_row.row.projected_values[i];
		child_values.emplace_back(names[4 + i],
		                          projected.present ? projected.value : projected_attributes[i].default_value);
	}
	return Value::STRUCT(std::move(child_values));
}

static Value H5LsBuildMapValue(const std::vector<H5TreeNamedRow> &rows,
                               const vector<H5TreeProjectedAttributeSpec> &projected_attributes,
                               const vector<string> &names, const vector<LogicalType> &return_types) {
	child_list_t<LogicalType> struct_fields;
	for (idx_t i = 0; i < names.size(); i++) {
		struct_fields.emplace_back(names[i], return_types[i]);
	}
	auto value_type = LogicalType::STRUCT(std::move(struct_fields));
	vector<Value> keys;
	vector<Value> values;
	keys.reserve(rows.size());
	values.reserve(rows.size());
	for (const auto &named_row : rows) {
		keys.emplace_back(Value(named_row.name));
		values.emplace_back(H5LsRowToStructValue(named_row, names, return_types, projected_attributes));
	}
	return Value::MAP(LogicalType::VARCHAR, value_type, std::move(keys), std::move(values));
}

static unique_ptr<FunctionData> H5LsBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5LsBindData>();
	result->swmr = ResolveSwmrOption(context, input.named_parameters);
	auto expanded = H5ExpandFilePatterns(context, input.inputs[0], "h5_ls");
	result->filenames = std::move(expanded.filenames);
	result->had_glob = expanded.had_glob;

	idx_t projected_attr_start = 1;
	if (input.inputs.size() >= 2 && input.inputs[1].IsNull()) {
		throw InvalidInputException("h5_ls path must not be NULL");
	}
	if (input.inputs.size() >= 2 && input.inputs[1].type().id() == LogicalTypeId::VARCHAR) {
		result->group_path = H5TreeNormalizeObjectPath(GetRequiredStringArgument(input.inputs[1], "h5_ls", "path"));
		projected_attr_start = 2;
	} else {
		result->group_path = "/";
	}

	H5LsGetReturnSchema(result->projected_attributes, names, return_types);
	H5TreeBindProjectedAttributes("h5_ls", input.inputs, projected_attr_start, names, return_types,
	                              result->projected_attributes);
	auto filename_option = ResolveFilenameColumnOption(input.named_parameters);
	if (filename_option.include) {
		if (H5LsOutputHasColumnName(names, filename_option.column_name)) {
			throw BinderException("Option filename adds column \"%s\", but that column name is already present in "
			                      "h5_ls output",
			                      filename_option.column_name);
		}
		result->visible_filename_idx = names.size();
		names.push_back(filename_option.column_name);
		return_types.push_back(LogicalType::VARCHAR);
	}
	return std::move(result);
}

static void H5LsOpenFileScanner(ClientContext &context, const H5LsBindData &bind_data, idx_t file_idx,
                                H5LsGlobalState &state) {
	ThrowIfInterrupted(context);
	D_ASSERT(file_idx < bind_data.filenames.size());
	state.file_idx = file_idx;
	state.scanner = make_uniq<H5LsFileScanner>(context, bind_data.filenames[file_idx], bind_data.group_path,
	                                           bind_data.swmr, bind_data.projected_attributes);
}

static unique_ptr<GlobalTableFunctionState> H5LsInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5LsBindData>();
	auto result = make_uniq<H5LsGlobalState>();
	result->output_layout = H5LsBuildOutputLayout(bind_data, input.column_ids);
	D_ASSERT(!bind_data.filenames.empty());
	H5LsOpenFileScanner(context, bind_data, 0, *result);
	return std::move(result);
}

static void H5LsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5LsBindData>();
	auto &gstate = input.global_state->Cast<H5LsGlobalState>();
	auto &rows = gstate.batch_rows;
	while (true) {
		D_ASSERT(gstate.scanner);
		auto exhausted = gstate.scanner->ReadRows(context, rows);
		if (!rows.empty()) {
			break;
		}
		D_ASSERT(exhausted);
		auto next_file_idx = gstate.file_idx + 1;
		if (next_file_idx >= bind_data.filenames.size()) {
			output.SetCardinality(0);
			return;
		}
		H5LsOpenFileScanner(context, bind_data, next_file_idx, gstate);
	}

	auto count = rows.size();
	output.SetCardinality(count);

	idx_t total_shape_elems = 0;
	idx_t shape_offset = 0;
	uint64_t *shape_data = nullptr;
	auto shape_output_idx = gstate.output_layout.shape_output_idx;
	if (shape_output_idx.has_value()) {
		for (idx_t i = 0; i < count; i++) {
			if (rows[i].row.has_shape) {
				total_shape_elems += rows[i].row.shape.size();
			}
		}
		auto &shape_vector = output.data[*shape_output_idx];
		ListVector::Reserve(shape_vector, total_shape_elems);
		auto &child = ListVector::GetEntry(shape_vector);
		shape_data = FlatVector::GetData<uint64_t>(child);
	}

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		for (const auto &output_column : gstate.output_layout.projected_columns) {
			H5TreeWriteProjectedValue(rows[row_idx].row, bind_data.projected_attributes, output_column.column_id,
			                          output.data[output_column.output_idx], row_idx, shape_offset, shape_data);
		}
	}
	if (shape_output_idx.has_value()) {
		ListVector::SetListSize(output.data[*shape_output_idx], shape_offset);
	}
	H5LsPopulateFilenameColumns(bind_data.filenames[gstate.file_idx], gstate.output_layout.filename_output_idxs,
	                            output);
}

static unique_ptr<FunctionData> H5LsScalarBindInternal(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments,
                                                       const char *function_name, bool force_swmr) {
	if (arguments.size() < 2) {
		throw InvalidInputException("%s requires at least 2 arguments: filename and group path", function_name);
	}
	for (idx_t i = 2; i < arguments.size(); i++) {
		if (!arguments[i]->IsFoldable()) {
			throw InvalidInputException("scalar %s projected attribute arguments must be constant expressions",
			                            function_name);
		}
	}

	vector<Value> projected_attribute_values;
	projected_attribute_values.reserve(arguments.size() - 2);
	for (idx_t i = 2; i < arguments.size(); i++) {
		projected_attribute_values.push_back(ExpressionExecutor::EvaluateScalar(context, *arguments[i]));
	}

	auto swmr = force_swmr ? true : ResolveSwmrOption(context, named_parameter_map_t {});

	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	vector<string> names;
	vector<LogicalType> return_types;
	H5LsGetReturnSchema(projected_attributes, names, return_types);
	if (!force_swmr) {
		for (auto &value : projected_attribute_values) {
			if (!H5TreeIsProjectedAttributeArgument(value)) {
				throw InvalidInputException("scalar h5_ls extra arguments must be h5_attr(), h5_attr(name), "
				                            "h5_attr(name, default_value) or h5_alias(alias, h5_attr(...)); named "
				                            "parameters are not supported");
			}
		}
	}
	H5TreeBindProjectedAttributes(function_name, projected_attribute_values, 0, names, return_types,
	                              projected_attributes);
	H5LsValidateUniqueFieldNames(names);

	child_list_t<LogicalType> struct_fields;
	for (idx_t i = 0; i < names.size(); i++) {
		struct_fields.emplace_back(names[i], return_types[i]);
	}
	auto return_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::STRUCT(std::move(struct_fields)));

	bound_function.return_type = return_type;
	return make_uniq<H5LsScalarBindData>(std::move(projected_attributes), swmr);
}

static unique_ptr<FunctionData> H5LsScalarBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	return H5LsScalarBindInternal(context, bound_function, arguments, "h5_ls", false);
}

static unique_ptr<FunctionData> H5LsSwmrScalarBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	return H5LsScalarBindInternal(context, bound_function, arguments, "h5_ls_swmr", true);
}

static void H5LsScalarWriteRow(H5TreeFileReader &reader, const string_t &path_value, Vector &result, idx_t row_idx,
                               const vector<H5TreeProjectedAttributeSpec> &projected_attributes,
                               const vector<string> &names, const vector<LogicalType> &return_types) {
	auto group_path = H5TreeNormalizeObjectPath(path_value.GetString());
	std::vector<H5TreeNamedRow> rows;
	H5TreeListImmediateEntries(reader, group_path, rows);
	result.SetValue(row_idx, H5LsBuildMapValue(rows, projected_attributes, names, return_types));
}

struct H5LsScalarFileRows {
	string filename;
	vector<idx_t> row_idxs;
};

static void H5LsScalarWriteFileRows(ClientContext &context, const H5LsScalarFileRows &file_rows,
                                    const UnifiedVectorFormat &path_data, const string_t *path_ptr, Vector &result,
                                    const H5LsScalarBindData &bind_data, const vector<string> &names,
                                    const vector<LogicalType> &return_types) {
	if (file_rows.row_idxs.empty()) {
		return;
	}
	ThrowIfInterrupted(context);
	H5TreeFileReader reader(context, file_rows.filename, bind_data.swmr, bind_data.projected_attributes);
	for (auto row_idx : file_rows.row_idxs) {
		auto path_idx = path_data.sel->get_index(row_idx);
		H5LsScalarWriteRow(reader, path_ptr[path_idx], result, row_idx, bind_data.projected_attributes, names,
		                   return_types);
	}
}

static void H5LsScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<H5LsScalarBindData>();
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

	vector<string> names;
	vector<LogicalType> return_types;
	H5LsGetReturnSchema(bind_data.projected_attributes, names, return_types);
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
		H5TreeFileReader reader(context, filename, bind_data.swmr, bind_data.projected_attributes);
		H5LsScalarWriteRow(reader, path_ptr[path_idx], result, 0, bind_data.projected_attributes, names, return_types);
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

		H5LsScalarFileRows file_rows;
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

		H5LsScalarWriteFileRows(context, file_rows, path_data, path_ptr, result, bind_data, names, return_types);
		return;
	}

	std::unordered_map<string, idx_t> file_group_lookup;
	vector<H5LsScalarFileRows> file_groups;
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
		H5LsScalarWriteFileRows(context, file_group, path_data, path_ptr, result, bind_data, names, return_types);
	}
}

void RegisterH5LsFunctions(ExtensionLoader &loader) {
	TableFunction h5_ls_table_function("h5_ls", {LogicalType::VARCHAR}, H5LsScan, H5LsBind, H5LsInit);
	h5_ls_table_function.varargs = LogicalType::ANY;
	h5_ls_table_function.named_parameters["filename"] = LogicalType::ANY;
	h5_ls_table_function.named_parameters["swmr"] = LogicalType::BOOLEAN;
	// Projection pushdown is enabled so DuckDB can bind hidden virtual columns.
	// h5_ls still intentionally collects full child metadata for each emitted row.
	h5_ls_table_function.projection_pushdown = true;
	h5_ls_table_function.get_virtual_columns = H5GetFilenameVirtualColumns;
	loader.RegisterFunction(MultiFileReader::CreateFunctionSet(std::move(h5_ls_table_function)));

	ScalarFunction h5_ls_scalar("h5_ls", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalTypeId::MAP,
	                            H5LsScalarFunction, H5LsScalarBind);
	h5_ls_scalar.varargs = LogicalType::ANY;
	h5_ls_scalar.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(h5_ls_scalar);

	ScalarFunction h5_ls_swmr_scalar("h5_ls_swmr", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalTypeId::MAP,
	                                 H5LsScalarFunction, H5LsSwmrScalarBind);
	h5_ls_swmr_scalar.varargs = LogicalType::ANY;
	h5_ls_swmr_scalar.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(h5_ls_swmr_scalar);
}

} // namespace duckdb
