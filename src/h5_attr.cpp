#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif

namespace duckdb {

template <class T>
static T &GetH5AttrStructChild(T &child) {
	return child;
}

template <class T>
static T &GetH5AttrStructChild(unique_ptr<T> &child) {
	return *child;
}

static void H5AttrFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 3);
	auto &tag_child = GetH5AttrStructChild(children[0]);
	auto &name_child = GetH5AttrStructChild(children[1]);
	auto &default_child = GetH5AttrStructChild(children[2]);

	if (args.ColumnCount() == 0) {
		for (idx_t i = 0; i < args.size(); i++) {
			FlatVector::GetData<string_t>(tag_child)[i] = StringVector::AddString(tag_child, "__attr_all__");
			FlatVector::SetNull(name_child, i, true);
			FlatVector::SetNull(default_child, i, true);
		}
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		result.Verify(args.size());
		return;
	}

	auto &name_vec = args.data[0];
	UnifiedVectorFormat name_data;
	name_vec.ToUnifiedFormat(args.size(), name_data);
	auto name_ptr = UnifiedVectorFormat::GetData<string_t>(name_data);
	for (idx_t i = 0; i < args.size(); i++) {
		auto name_idx = name_data.sel->get_index(i);
		if (!name_data.validity.RowIsValid(name_idx)) {
			throw InvalidInputException("h5_attr name must not be NULL");
		}
		FlatVector::GetData<string_t>(tag_child)[i] = StringVector::AddString(tag_child, "__attr__");
		FlatVector::GetData<string_t>(name_child)[i] = StringVector::AddString(name_child, name_ptr[name_idx]);
	}

	bool all_const = name_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	if (args.ColumnCount() == 2) {
		auto &default_vec = args.data[1];
		default_child.Reference(default_vec);
		all_const = all_const && default_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	} else if (all_const) {
		default_child.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(default_child, true);
	} else {
		for (idx_t i = 0; i < args.size(); i++) {
			FlatVector::SetNull(default_child, i, true);
		}
	}

	result.SetVectorType(all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	result.Verify(args.size());
}

static unique_ptr<FunctionData> H5AttrBind(ClientContext &, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() > 2) {
		throw InvalidInputException("h5_attr() accepts zero, one, or two arguments");
	}
	LogicalType default_type;
	LogicalType attribute_name_type = LogicalType::VARCHAR;
	if (arguments.size() == 2) {
		if (!arguments[1]->IsFoldable()) {
			throw InvalidInputException("h5_attr default_value must be a constant expression");
		}
		if (arguments[1]->return_type.id() == LogicalTypeId::SQLNULL) {
			throw InvalidInputException(
			    "h5_attr default_value must have a concrete type; use an explicit cast such as NULL::VARCHAR");
		}
		default_type = arguments[1]->return_type;
		attribute_name_type = arguments[0]->return_type;
	} else if (arguments.size() == 1) {
		default_type = LogicalType::VARIANT();
		attribute_name_type = arguments[0]->return_type;
	} else {
		default_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARIANT());
	}
	child_list_t<LogicalType> struct_children = {
	    {"tag", LogicalType::VARCHAR}, {"attribute_name", attribute_name_type}, {"default_value", default_type}};
	bound_function.return_type = LogicalType::STRUCT(struct_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

void RegisterH5AttrFunction(ExtensionLoader &loader) {
	ScalarFunctionSet h5_attr("h5_attr");

	ScalarFunction h5_attr_zero("h5_attr", {}, LogicalTypeId::STRUCT, H5AttrFunction, H5AttrBind);
	h5_attr_zero.serialize = VariableReturnBindData::Serialize;
	h5_attr_zero.deserialize = VariableReturnBindData::Deserialize;
	h5_attr_zero.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	h5_attr.AddFunction(h5_attr_zero);

	ScalarFunction h5_attr_one("h5_attr", {LogicalType::VARCHAR}, LogicalTypeId::STRUCT, H5AttrFunction, H5AttrBind);
	h5_attr_one.serialize = VariableReturnBindData::Serialize;
	h5_attr_one.deserialize = VariableReturnBindData::Deserialize;
	h5_attr_one.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	h5_attr.AddFunction(h5_attr_one);

	ScalarFunction h5_attr_two("h5_attr", {LogicalType::VARCHAR, LogicalType::ANY}, LogicalTypeId::STRUCT,
	                           H5AttrFunction, H5AttrBind);
	h5_attr_two.serialize = VariableReturnBindData::Serialize;
	h5_attr_two.deserialize = VariableReturnBindData::Deserialize;
	h5_attr_two.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	h5_attr.AddFunction(h5_attr_two);

	CreateScalarFunctionInfo info(std::move(h5_attr));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(H5FunctionDescription(
	    {}, {"name", "default_value"}, "Creates a projected HDF5 attribute definition for h5_tree() and h5_ls().",
	    {"FROM h5_tree('data.h5', h5_attr('NX_class'))"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
