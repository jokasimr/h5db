#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_tree_shared.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include <algorithm>
#include <vector>

namespace duckdb {

struct H5LsBindData : public TableFunctionData {
	std::string filename;
	std::string group_path;
	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	bool swmr = false;
};

struct H5LsGlobalState : public GlobalTableFunctionState {
	std::vector<H5TreeNamedRow> rows;
	idx_t offset = 0;
};

struct H5LsScalarBindData : public FunctionData {
	std::string filename;
	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	bool swmr = false;
	LogicalType return_type;

	H5LsScalarBindData(std::string filename_p, vector<H5TreeProjectedAttributeSpec> projected_attributes_p, bool swmr_p,
	                   LogicalType return_type_p)
	    : filename(std::move(filename_p)), projected_attributes(std::move(projected_attributes_p)), swmr(swmr_p),
	      return_type(std::move(return_type_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<H5LsScalarBindData>(filename, projected_attributes, swmr, return_type);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<H5LsScalarBindData>();
		if (filename != other.filename || swmr != other.swmr || return_type != other.return_type ||
		    projected_attributes.size() != other.projected_attributes.size()) {
			return false;
		}
		for (idx_t i = 0; i < projected_attributes.size(); i++) {
			const auto &lhs = projected_attributes[i];
			const auto &rhs = other.projected_attributes[i];
			if (lhs.attribute_name != rhs.attribute_name || lhs.output_column_name != rhs.output_column_name ||
			    lhs.output_type != rhs.output_type || lhs.default_value.ToString() != rhs.default_value.ToString()) {
				return false;
			}
		}
		return true;
	}
};

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
	if (input.inputs.empty()) {
		throw InvalidInputException("h5_ls requires at least 1 argument: filename");
	}
	auto result = make_uniq<H5LsBindData>();
	result->filename = GetRequiredStringArgument(input.inputs[0], "h5_ls", "filename");
	result->swmr = ResolveSwmrOption(context, input.named_parameters);

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
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> H5LsInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5LsBindData>();
	auto result = make_uniq<H5LsGlobalState>();
	H5TreeFileReader reader(context, bind_data.filename, bind_data.swmr, bind_data.projected_attributes);
	H5TreeListImmediateEntries(reader, bind_data.group_path, result->rows);
	return std::move(result);
}

static void H5LsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5LsBindData>();
	auto &gstate = input.global_state->Cast<H5LsGlobalState>();
	if (gstate.offset >= gstate.rows.size()) {
		output.SetCardinality(0);
		return;
	}

	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, gstate.rows.size() - gstate.offset);
	output.SetCardinality(count);

	idx_t total_shape_elems = 0;
	for (idx_t i = 0; i < count; i++) {
		if (gstate.rows[gstate.offset + i].row.has_shape) {
			total_shape_elems += gstate.rows[gstate.offset + i].row.shape.size();
		}
	}

	auto &shape_vector = output.data[3];
	ListVector::Reserve(shape_vector, total_shape_elems);
	auto &child = ListVector::GetEntry(shape_vector);
	auto *shape_data = FlatVector::GetData<uint64_t>(child);
	idx_t shape_offset = 0;

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		H5TreeWriteRow(gstate.rows[gstate.offset + row_idx].row, bind_data.projected_attributes, output, row_idx,
		               shape_offset, shape_data);
	}
	ListVector::SetListSize(shape_vector, shape_offset);
	gstate.offset += count;
}

static unique_ptr<FunctionData> H5LsScalarBindInternal(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments,
                                                       const char *function_name, bool force_swmr) {
	if (arguments.size() < 2) {
		throw InvalidInputException("%s requires at least 2 arguments: filename and group path", function_name);
	}
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("scalar %s filename must be a constant expression", function_name);
	}
	for (idx_t i = 2; i < arguments.size(); i++) {
		if (!arguments[i]->IsFoldable()) {
			throw InvalidInputException("scalar %s projected attribute arguments must be constant expressions",
			                            function_name);
		}
	}

	vector<Value> values;
	values.reserve(arguments.size());
	values.push_back(ExpressionExecutor::EvaluateScalar(context, *arguments[0]));
	values.push_back(Value(LogicalType::VARCHAR));
	for (idx_t i = 2; i < arguments.size(); i++) {
		values.push_back(ExpressionExecutor::EvaluateScalar(context, *arguments[i]));
	}

	auto filename = GetRequiredStringArgument(values[0], function_name, "filename");
	auto swmr = force_swmr ? true : ResolveSwmrOption(context, named_parameter_map_t {});

	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	vector<string> names;
	vector<LogicalType> return_types;
	H5LsGetReturnSchema(projected_attributes, names, return_types);
	if (!force_swmr) {
		for (idx_t i = 2; i < values.size(); i++) {
			if (!H5TreeIsProjectedAttributeArgument(values[i])) {
				throw InvalidInputException("scalar h5_ls extra arguments must be h5_attr(name) or "
				                            "h5_attr(name, default_value) or h5_alias(alias, h5_attr(...)); named "
				                            "parameters such as swmr := true are not supported");
			}
		}
	}
	H5TreeBindProjectedAttributes(function_name, values, 2, names, return_types, projected_attributes);

	child_list_t<LogicalType> struct_fields;
	for (idx_t i = 0; i < names.size(); i++) {
		struct_fields.emplace_back(names[i], return_types[i]);
	}
	auto return_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::STRUCT(std::move(struct_fields)));

	bound_function.return_type = return_type;
	return make_uniq<H5LsScalarBindData>(std::move(filename), std::move(projected_attributes), swmr,
	                                     std::move(return_type));
}

static unique_ptr<FunctionData> H5LsScalarBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	return H5LsScalarBindInternal(context, bound_function, arguments, "h5_ls", false);
}

static unique_ptr<FunctionData> H5LsSwmrScalarBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	return H5LsScalarBindInternal(context, bound_function, arguments, "h5_ls_swmr", true);
}

static void H5LsScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<H5LsScalarBindData>();
	auto &path_vec = args.data[1];
	UnifiedVectorFormat path_data;
	path_vec.ToUnifiedFormat(args.size(), path_data);
	auto path_ptr = UnifiedVectorFormat::GetData<string_t>(path_data);
	vector<string> names;
	vector<LogicalType> return_types;
	H5LsGetReturnSchema(bind_data.projected_attributes, names, return_types);
	H5TreeFileReader reader(state.GetContext(), bind_data.filename, bind_data.swmr, bind_data.projected_attributes);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < args.size(); i++) {
		auto path_idx = path_data.sel->get_index(i);
		if (!path_data.validity.RowIsValid(path_idx)) {
			validity.SetInvalid(i);
			continue;
		}
		validity.SetValid(i);
		auto group_path = H5TreeNormalizeObjectPath(path_ptr[path_idx].GetString());
		std::vector<H5TreeNamedRow> rows;
		H5TreeListImmediateEntries(reader, group_path, rows);
		result.SetValue(i, H5LsBuildMapValue(rows, bind_data.projected_attributes, names, return_types));
	}
}

void RegisterH5LsFunctions(ExtensionLoader &loader) {
	TableFunction h5_ls_table("h5_ls", {LogicalType::VARCHAR}, H5LsScan, H5LsBind, H5LsInit);
	h5_ls_table.varargs = LogicalType::ANY;
	h5_ls_table.named_parameters["swmr"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(h5_ls_table);

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
