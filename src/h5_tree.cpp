#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include <vector>
#include <string>
#include <optional>

namespace duckdb {

template <class T>
static T &GetStructChild(T &child) {
	return child;
}

template <class T>
static T &GetStructChild(unique_ptr<T> &child) {
	return *child;
}

struct ProjectedAttributeValue {
	bool present = false;
	Value value;
};

struct H5ObjectInfo {
	std::string path;
	std::string type;           // "group" or "dataset"
	std::string dtype;          // data type (for datasets)
	std::vector<hsize_t> shape; // shape (for datasets)
	std::vector<ProjectedAttributeValue> projected_values;
};

struct H5TreeProjectedAttributeSpec {
	std::string attribute_name;
	std::string output_column_name;
	Value default_value;
	LogicalType output_type;
};

struct H5TreeBindData : public TableFunctionData {
	std::string filename;
	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	mutable std::vector<H5ObjectInfo> objects;
	mutable bool scanned = false;
	bool swmr = false;
};

struct H5TreeGlobalState : public GlobalTableFunctionState {
	idx_t position = 0;
};

struct H5TreeVisitData {
	std::vector<H5ObjectInfo> *objects;
	const vector<H5TreeProjectedAttributeSpec> *projected_attributes;
	ClientContext *context;
	hid_t file_id;
	std::string error_message;
	bool error = false;
};

static bool IsAliasStructType(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	if (children.size() != 3) {
		return false;
	}
	return children[0].second == LogicalType::VARCHAR;
}

static bool IsAttrStructType(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	if (children.size() != 3) {
		return false;
	}
	return children[0].second == LogicalType::VARCHAR;
}

static Value UnwrapAliasSpec(const Value &input, std::optional<std::string> &alias_name) {
	Value current = input;
	while (IsAliasStructType(current.type())) {
		auto &children = StructValue::GetChildren(current);
		if (children.size() != 3) {
			throw InvalidInputException("h5_alias() must return a struct with 3 fields");
		}
		if (children[0].GetValue<string>() != "__alias__") {
			break;
		}
		if (!alias_name) {
			alias_name = children[1].GetValue<string>();
		}
		current = children[2];
	}
	return current;
}

static H5TreeProjectedAttributeSpec ParseProjectedAttributeSpec(const Value &input) {
	std::optional<std::string> alias_name;
	auto current = UnwrapAliasSpec(input, alias_name);
	if (!IsAttrStructType(current.type())) {
		throw InvalidInputException("h5_tree projected attribute arguments must be h5_attr(name, default_value) or "
		                            "h5_alias(alias, h5_attr(...))");
	}

	auto &children = StructValue::GetChildren(current);
	if (children.size() != 3 || children[0].GetValue<string>() != "__attr__") {
		throw InvalidInputException("h5_tree projected attribute arguments must be h5_attr(name, default_value) or "
		                            "h5_alias(alias, h5_attr(...))");
	}
	if (children[1].IsNull()) {
		throw InvalidInputException("h5_attr name must not be NULL");
	}
	if (children[2].type().id() == LogicalTypeId::SQLNULL) {
		throw InvalidInputException(
		    "h5_attr default_value must have a concrete type; use an explicit cast such as NULL::VARCHAR");
	}

	H5TreeProjectedAttributeSpec spec;
	spec.attribute_name = children[1].GetValue<string>();
	spec.output_column_name = alias_name ? *alias_name : spec.attribute_name;
	spec.default_value = children[2];
	spec.output_type = children[2].type();
	return spec;
}

static std::string NormalizeExceptionMessage(const std::string &message) {
	if (message.empty() || message.front() != '{') {
		return message;
	}
	try {
		auto info = StringUtil::ParseJSONMap(message, true)->Flatten();
		for (const auto &entry : info) {
			if (entry.first == "exception_message") {
				return entry.second;
			}
		}
	} catch (...) {
	}
	return message;
}

static void WriteShapeListRow(Vector &shape_vector, idx_t row_idx, const H5ObjectInfo &obj, idx_t &shape_offset,
                              uint64_t *shape_data) {
	auto shape_entries = ListVector::GetData(shape_vector);
	auto &shape_validity = FlatVector::Validity(shape_vector);

	if (obj.type == "group") {
		shape_validity.SetInvalid(row_idx);
		shape_entries[row_idx].offset = 0;
		shape_entries[row_idx].length = 0;
		return;
	}

	shape_validity.SetValid(row_idx);
	shape_entries[row_idx].offset = shape_offset;
	shape_entries[row_idx].length = obj.shape.size();
	for (auto dim : obj.shape) {
		shape_data[shape_offset++] = static_cast<uint64_t>(dim);
	}
}

static void PopulateDatasetMetadata(H5ObjectInfo &obj_info, hid_t dataset_id) {
	hid_t type_id = H5Dget_type(dataset_id);
	if (type_id < 0) {
		throw IOException("Failed to get dataset type during tree traversal: " + obj_info.path);
	}
	H5TypeHandle type = H5TypeHandle::TakeOwnershipOf(type_id);
	obj_info.dtype = H5TypeToString(type);
	obj_info.shape = H5GetShape(dataset_id);
}

static void PopulateProjectedAttributes(H5ObjectInfo &obj_info, hid_t object_id, const H5TreeVisitData &visit_data) {
	for (idx_t attr_idx = 0; attr_idx < visit_data.projected_attributes->size(); attr_idx++) {
		const auto &spec = (*visit_data.projected_attributes)[attr_idx];
		auto exists = H5Aexists(object_id, spec.attribute_name.c_str());
		if (exists < 0) {
			throw IOException("Failed to inspect attribute: " + spec.attribute_name);
		}
		if (exists == 0) {
			obj_info.projected_values[attr_idx].present = false;
			continue;
		}

		H5AttributeHandle attr(object_id, spec.attribute_name.c_str());
		if (!attr.is_valid()) {
			throw IOException("Failed to open attribute: " + spec.attribute_name);
		}
		hid_t type_id = H5Aget_type(attr);
		if (type_id < 0) {
			throw IOException("Failed to get type for attribute: " + spec.attribute_name);
		}
		H5TypeHandle type = H5TypeHandle::TakeOwnershipOf(type_id);
		hid_t space_id = H5Aget_space(attr);
		if (space_id < 0) {
			throw IOException("Failed to get dataspace for attribute: " + spec.attribute_name);
		}
		H5DataspaceHandle space = H5DataspaceHandle::TakeOwnershipOf(space_id);

		auto source_type = H5ResolveAttributeLogicalType(type.get(), space.get(), spec.attribute_name);
		auto value = H5ReadAttributeValue(attr, type.get(), source_type, spec.attribute_name);
		try {
			value = value.CastAs(*visit_data.context, spec.output_type);
		} catch (...) {
			throw IOException("Attribute '" + spec.attribute_name + "' contains values that cannot be cast to " +
			                  spec.output_type.ToString());
		}

		obj_info.projected_values[attr_idx].present = true;
		obj_info.projected_values[attr_idx].value = std::move(value);
	}
}

static herr_t visit_callback(hid_t obj_id, const char *name, const H5O_info_t *info, void *op_data) {
	auto &visit_data = *reinterpret_cast<H5TreeVisitData *>(op_data);
	if (visit_data.error) {
		return -1;
	}
	auto fail = [&](std::string message) {
		visit_data.error = true;
		visit_data.error_message = std::move(message);
		return -1;
	};
	auto &objects = *visit_data.objects;

	H5ObjectInfo obj_info;
	obj_info.path = std::string(name) == "." ? "/" : std::string("/") + name;
	obj_info.projected_values.resize(visit_data.projected_attributes->size());
	auto has_projected_attributes = !visit_data.projected_attributes->empty();

	if (info->type == H5O_TYPE_GROUP) {
		obj_info.type = "group";
		obj_info.dtype = "";
		obj_info.shape = {};
		if (has_projected_attributes) {
			try {
				H5ObjectHandle current_object(visit_data.file_id, obj_info.path.c_str());
				if (!current_object.is_valid()) {
					return fail("Failed to open object during tree traversal: " + obj_info.path);
				}
				PopulateProjectedAttributes(obj_info, current_object, visit_data);
			} catch (const std::exception &ex) {
				return fail(NormalizeExceptionMessage(ex.what()));
			}
		}
	} else if (info->type == H5O_TYPE_DATASET) {
		obj_info.type = "dataset";

		try {
			H5ErrorSuppressor suppress;
			H5DatasetHandle dataset(obj_id, name);
			if (!dataset.is_valid()) {
				return fail("Failed to open dataset during tree traversal: " + obj_info.path);
			}
			PopulateDatasetMetadata(obj_info, dataset);
			if (has_projected_attributes) {
				PopulateProjectedAttributes(obj_info, dataset, visit_data);
			}
		} catch (const std::exception &ex) {
			return fail(NormalizeExceptionMessage(ex.what()));
		}
	}

	objects.push_back(obj_info);
	return 0; // Continue iteration
}

static unique_ptr<FunctionData> H5TreeBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5TreeBindData>();

	if (input.inputs.empty()) {
		throw InvalidInputException("h5_tree requires at least 1 argument: filename");
	}
	result->filename = input.inputs[0].GetValue<string>();
	result->swmr = ResolveSwmrOption(context, input.named_parameters);

	names = {"path", "type", "dtype", "shape"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::UBIGINT)};

	for (idx_t i = 1; i < input.inputs.size(); i++) {
		auto spec = ParseProjectedAttributeSpec(input.inputs[i]);
		names.push_back(spec.output_column_name);
		return_types.push_back(spec.output_type);
		result->projected_attributes.push_back(std::move(spec));
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> H5TreeInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5TreeBindData>();
	auto result = make_uniq<H5TreeGlobalState>();

	if (!bind_data.scanned) {
		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

		H5FileHandle file;
		{
			H5ErrorSuppressor suppress;
			file = H5FileHandle(&context, bind_data.filename.c_str(), H5F_ACC_RDONLY, bind_data.swmr);
		}

		if (!file.is_valid()) {
			throw IOException(FormatRemoteFileError("Failed to open HDF5 file", bind_data.filename));
		}

		H5O_info_t obj_info;
		H5ErrorSuppressor suppress;
		if (H5Oget_info(file, &obj_info, H5O_INFO_BASIC) < 0) {
			throw IOException(FormatRemoteFileError("Failed to inspect HDF5 root object", bind_data.filename));
		}
		H5TreeVisitData visit_data;
		visit_data.objects = &bind_data.objects;
		visit_data.projected_attributes = &bind_data.projected_attributes;
		visit_data.context = &context;
		visit_data.file_id = file;
		auto status = H5Ovisit(file, H5_INDEX_NAME, H5_ITER_NATIVE, visit_callback, &visit_data, H5O_INFO_BASIC);
		if (status < 0) {
			if (visit_data.error && !visit_data.error_message.empty()) {
				throw IOException(AppendRemoteError(visit_data.error_message, bind_data.filename));
			}
			throw IOException(FormatRemoteFileError("Failed to traverse HDF5 objects", bind_data.filename));
		}
		if (bind_data.objects.empty()) {
			throw IOException(FormatRemoteFileError("Failed to traverse HDF5 objects", bind_data.filename));
		}

		bind_data.scanned = true;
	}

	return std::move(result);
}

static void H5TreeScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &bind_data = data.bind_data->Cast<H5TreeBindData>();
	auto &gstate = data.global_state->Cast<H5TreeGlobalState>();

	idx_t count = 0;
	idx_t remaining = bind_data.objects.size() - gstate.position;
	idx_t to_process = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

	if (to_process == 0) {
		output.SetCardinality(0);
		return;
	}

	auto &path_vector = output.data[0];
	auto &type_vector = output.data[1];
	auto &dtype_vector = output.data[2];
	auto &shape_vector = output.data[3];
	auto &shape_child = ListVector::GetEntry(shape_vector);
	idx_t total_shape_elems = 0;
	for (idx_t i = 0; i < to_process; i++) {
		const auto &obj = bind_data.objects[gstate.position + i];
		if (obj.type != "group") {
			total_shape_elems += obj.shape.size();
		}
	}
	ListVector::Reserve(shape_vector, total_shape_elems);
	auto shape_data = FlatVector::GetData<uint64_t>(shape_child);
	idx_t shape_offset = 0;

	for (idx_t i = 0; i < to_process; i++) {
		const auto &obj = bind_data.objects[gstate.position + i];

		FlatVector::GetData<string_t>(path_vector)[i] = StringVector::AddString(path_vector, obj.path);
		FlatVector::GetData<string_t>(type_vector)[i] = StringVector::AddString(type_vector, obj.type);
		FlatVector::GetData<string_t>(dtype_vector)[i] = StringVector::AddString(dtype_vector, obj.dtype);
		WriteShapeListRow(shape_vector, i, obj, shape_offset, shape_data);
		for (idx_t attr_idx = 0; attr_idx < bind_data.projected_attributes.size(); attr_idx++) {
			auto &result_vector = output.data[4 + attr_idx];
			const auto &projected = obj.projected_values[attr_idx];
			if (projected.present) {
				result_vector.SetValue(i, projected.value);
			} else {
				result_vector.SetValue(i, bind_data.projected_attributes[attr_idx].default_value);
			}
		}

		count++;
	}
	ListVector::SetListSize(shape_vector, shape_offset);

	gstate.position += count;
	output.SetCardinality(count);
}

static void H5AttrFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vec = args.data[0];
	auto &default_vec = args.data[1];

	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 3);
	auto &tag_child = GetStructChild(children[0]);
	auto &name_child = GetStructChild(children[1]);
	auto &default_child = GetStructChild(children[2]);
	default_child.Reference(default_vec);

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

	bool all_const = default_vec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	                 name_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	result.SetVectorType(all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	result.Verify(args.size());
}

static unique_ptr<FunctionData> H5AttrBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw InvalidInputException("h5_attr() requires two arguments: attribute name and default value");
	}
	if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("h5_attr default_value must be a constant expression");
	}
	if (arguments[1]->return_type.id() == LogicalTypeId::SQLNULL) {
		throw InvalidInputException(
		    "h5_attr default_value must have a concrete type; use an explicit cast such as NULL::VARCHAR");
	}
	child_list_t<LogicalType> struct_children = {{"tag", LogicalType::VARCHAR},
	                                             {"attribute_name", arguments[0]->return_type},
	                                             {"default_value", arguments[1]->return_type}};
	bound_function.return_type = LogicalType::STRUCT(struct_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

void RegisterH5AttrFunction(ExtensionLoader &loader) {
	ScalarFunction h5_attr("h5_attr", {LogicalType::VARCHAR, LogicalType::ANY}, LogicalTypeId::STRUCT, H5AttrFunction,
	                       H5AttrBind);
	h5_attr.serialize = VariableReturnBindData::Serialize;
	h5_attr.deserialize = VariableReturnBindData::Deserialize;
	h5_attr.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(h5_attr);
}

void RegisterH5TreeFunction(ExtensionLoader &loader) {
	TableFunction h5_tree("h5_tree", {LogicalType::VARCHAR}, H5TreeScan, H5TreeBind, H5TreeInit);
	h5_tree.name = "h5_tree";
	h5_tree.varargs = LogicalType::ANY;
	h5_tree.named_parameters["swmr"] = LogicalType::BOOLEAN;

	loader.RegisterFunction(h5_tree);
}

} // namespace duckdb
