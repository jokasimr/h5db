#include "h5_functions.hpp"
#include "h5_internal.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

struct H5FirstFileBindData final : public FunctionData {
	explicit H5FirstFileBindData(bool in_macro_definition_p) : in_macro_definition(in_macro_definition_p) {
	}

	bool in_macro_definition;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<H5FirstFileBindData>(in_macro_definition);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<H5FirstFileBindData>();
		return in_macro_definition == other.in_macro_definition;
	}
};

static unique_ptr<FunctionData> H5FirstFileBind(ScalarFunctionBindInput &input, ScalarFunction &,
                                                vector<unique_ptr<Expression>> &) {
	return make_uniq<H5FirstFileBindData>(input.binder.macro_binding);
}

static unique_ptr<Expression> H5FirstFileBindExpression(FunctionBindExpressionInput &input) {
	auto &bind_data = input.bind_data->Cast<H5FirstFileBindData>();
	if (bind_data.in_macro_definition) {
		throw ParameterNotResolvedException();
	}

	auto &argument = input.children[0];
	if (argument->HasParameter() || argument->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	auto foldable = argument->IsFoldable();
	if (!foldable) {
		throw InvalidInputException("h5_first_file input must be a constant expression");
	}

	auto filename_input = ExpressionExecutor::EvaluateScalar(input.context, *argument);
	auto expanded = H5ExpandFilePatterns(input.context, filename_input, "h5_first_file");
	if (expanded.filenames.empty()) {
		throw IOException("h5_first_file found no files");
	}

	return make_uniq<BoundConstantExpression>(Value(expanded.filenames[0]));
}

static ScalarFunction CreateH5FirstFileFunction(LogicalType input_type) {
	ScalarFunction function("h5_first_file", {std::move(input_type)}, LogicalType::VARCHAR, nullptr);
	function.SetBindExtendedCallback(H5FirstFileBind);
	function.SetBindExpressionCallback(H5FirstFileBindExpression);
	return function;
}

void RegisterH5FirstFileFunction(ExtensionLoader &loader) {
	ScalarFunctionSet h5_first_file("h5_first_file");
	h5_first_file.AddFunction(CreateH5FirstFileFunction(LogicalType::VARCHAR));
	h5_first_file.AddFunction(CreateH5FirstFileFunction(LogicalType::LIST(LogicalType::VARCHAR)));
	CreateScalarFunctionInfo info(std::move(h5_first_file));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(
	    H5FunctionDescription({LogicalType::ANY}, {"filename_or_filenames"},
	                          "Returns the first concrete HDF5 filename from an exact path, glob, or list for "
	                          "planning-time use with h5_read().",
	                          {"FROM h5_read(h5_first_file('runs/run_*.h5'), '/detector_geometry')"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
