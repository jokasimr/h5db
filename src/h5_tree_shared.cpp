#include "h5_tree_shared.hpp"
#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "duckdb/common/exception.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif

namespace duckdb {

static bool H5TreeIsTaggedStructType(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	return children.size() == 3 && children[0].second == LogicalType::VARCHAR;
}

static string H5TreeProjectedAttributeUsage(const string &function_name) {
	return function_name + " projected attribute arguments must be h5_attr(), h5_attr(name), "
	                       "h5_attr(name, default_value) or h5_alias(alias, h5_attr(...))";
}

static Value H5TreeUnwrapAliasSpec(const Value &input, std::optional<std::string> &alias_name) {
	Value current = input;
	while (H5TreeIsTaggedStructType(current.type())) {
		auto &children = StructValue::GetChildren(current);
		if (children.size() != 3 || children[0].GetValue<string>() != "__alias__") {
			break;
		}
		if (!alias_name) {
			alias_name = children[1].GetValue<string>();
		}
		current = children[2];
	}
	return current;
}

bool H5TreeIsProjectedAttributeArgument(const Value &input) {
	std::optional<std::string> alias_name;
	auto current = H5TreeUnwrapAliasSpec(input, alias_name);
	if (!H5TreeIsTaggedStructType(current.type())) {
		return false;
	}
	auto &children = StructValue::GetChildren(current);
	if (children.size() != 3) {
		return false;
	}
	auto tag = children[0].GetValue<string>();
	return tag == "__attr__" || tag == "__attr_all__";
}

static H5TreeObjectIdentity H5TreeIdentityFromObjectInfo(const H5O_info2_t &info) {
	H5TreeObjectIdentity identity;
	identity.fileno = info.fileno;
	identity.token = info.token;
	return identity;
}

static void H5TreeWriteOptionalString(Vector &vector, idx_t row_idx, const std::optional<std::string> &value) {
	auto &validity = FlatVector::Validity(vector);
	if (!value) {
		validity.SetInvalid(row_idx);
		return;
	}
	validity.SetValid(row_idx);
	FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddString(vector, *value);
}

static void H5TreeWriteString(Vector &vector, idx_t row_idx, const std::string &value) {
	FlatVector::Validity(vector).SetValid(row_idx);
	FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddString(vector, value);
}

static void H5TreeWriteShapeRow(Vector &shape_vector, idx_t row_idx, const H5TreeRow &row, idx_t &shape_offset,
                                uint64_t *shape_data) {
	auto entries = ListVector::GetData(shape_vector);
	auto &validity = FlatVector::Validity(shape_vector);
	if (!row.shape) {
		validity.SetInvalid(row_idx);
		entries[row_idx].offset = 0;
		entries[row_idx].length = 0;
		return;
	}
	validity.SetValid(row_idx);
	entries[row_idx].offset = shape_offset;
	entries[row_idx].length = row.shape->size();
	for (auto dim : *row.shape) {
		shape_data[shape_offset++] = static_cast<uint64_t>(dim);
	}
}

void H5TreeWriteProjectedValue(const H5TreeRow &row, const vector<H5TreeProjectedAttributeSpec> &projected_attributes,
                               column_t column_id, Vector &vector, idx_t row_idx, idx_t &shape_offset,
                               uint64_t *shape_data) {
	switch (column_id) {
	case 0:
		H5TreeWriteString(vector, row_idx, row.path);
		break;
	case 1:
		H5TreeWriteOptionalString(vector, row_idx, row.type);
		break;
	case 2:
		H5TreeWriteOptionalString(vector, row_idx, row.dtype);
		break;
	case 3:
		D_ASSERT(shape_data);
		H5TreeWriteShapeRow(vector, row_idx, row, shape_offset, shape_data);
		break;
	default: {
		if (column_id < 4 || column_id >= 4 + projected_attributes.size()) {
			throw InternalException("h5_tree projected column id %llu out of range", column_id);
		}
		auto attr_idx = column_id - 4;
		const auto &projected = row.projected_values[attr_idx];
		if (projected.present) {
			vector.SetValue(row_idx, projected.value);
		} else {
			vector.SetValue(row_idx, projected_attributes[attr_idx].default_value);
		}
		break;
	}
	}
}

static bool H5TreeTypePreservesRawStringBytes(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::VARIANT:
	case LogicalTypeId::BLOB:
		return true;
	case LogicalTypeId::LIST: {
		auto &child_type = ListType::GetChildType(type);
		return H5TreeTypePreservesRawStringBytes(child_type);
	}
	case LogicalTypeId::ARRAY: {
		auto &child_type = ArrayType::GetChildType(type);
		return H5TreeTypePreservesRawStringBytes(child_type);
	}
	default:
		return false;
	}
}

static H5StringDecodeMode H5TreeProjectedAttributeDecodeMode(const LogicalType &output_type) {
	return H5TreeTypePreservesRawStringBytes(output_type) ? H5StringDecodeMode::TEXT_OR_BLOB
	                                                      : H5StringDecodeMode::STRICT_TEXT;
}

static void H5TreePopulateProjectedAttributeValue(H5TreeProjectedAttributeValue &target, hid_t object_id,
                                                  const H5TreeProjectedAttributeSpec &spec) {
	D_ASSERT(!spec.all_attributes);
	auto exists = H5Aexists(object_id, spec.attribute_name.c_str());
	if (exists < 0) {
		throw IOException("Failed to inspect attribute: " + spec.attribute_name);
	}
	if (exists == 0) {
		target.present = false;
		return;
	}

	auto opened = H5OpenAttribute(object_id, spec.attribute_name);
	auto source_type = H5ResolveAttributeLogicalType(opened.type.get(), opened.space.get(), spec.attribute_name);
	auto value = H5ReadAttributeValue(opened.attr, opened.type.get(), opened.space.get(), source_type,
	                                  spec.attribute_name, H5TreeProjectedAttributeDecodeMode(spec.output_type));
	Value cast_value;
	string error_message;
	if (!value.DefaultTryCastAs(spec.output_type, cast_value, &error_message, false)) {
		throw IOException("Attribute '" + spec.attribute_name + "' contains values that cannot be cast to " +
		                  spec.output_type.ToString());
	}
	target.present = true;
	target.value = std::move(cast_value);
}

struct H5TreeAllAttributesIterData {
	vector<Value> *keys = nullptr;
	vector<Value> *values = nullptr;
	bool error = false;
	string error_message;
};

static herr_t H5TreeAllAttributesCallback(hid_t location_id, const char *attr_name, const H5A_info_t *, void *op_data) {
	auto &iter_data = *reinterpret_cast<H5TreeAllAttributesIterData *>(op_data);
	if (iter_data.error) {
		return -1;
	}

	auto fail = [&](const string &message) {
		iter_data.error = true;
		iter_data.error_message = message;
		return -1;
	};

	try {
		auto attribute_name = string(attr_name);
		auto opened = H5OpenAttribute(location_id, attribute_name);
		LogicalType source_type;
		Value variant_value(LogicalType::VARIANT());
		if (!H5TryResolveAttributeLogicalType(opened.type.get(), opened.space.get(), source_type)) {
			iter_data.keys->emplace_back(Value(attribute_name));
			iter_data.values->push_back(std::move(variant_value));
			return 0;
		}

		auto value = H5ReadAttributeValue(opened.attr, opened.type.get(), opened.space.get(), source_type,
		                                  attribute_name, H5StringDecodeMode::TEXT_OR_BLOB);
		string error_message;
		if (!value.DefaultTryCastAs(LogicalType::VARIANT(), variant_value, &error_message, false)) {
			return fail("Attribute '" + attribute_name + "' contains values that cannot be cast to VARIANT");
		}
		iter_data.keys->emplace_back(Value(attribute_name));
		iter_data.values->push_back(std::move(variant_value));
	} catch (const std::exception &ex) {
		return fail(H5NormalizeExceptionMessage(ex.what()));
	}
	return 0;
}

Value H5ReadAllAttributesMapValue(hid_t object_id) {
	vector<Value> keys;
	vector<Value> values;

	hsize_t idx = 0;
	H5TreeAllAttributesIterData iter_data;
	iter_data.keys = &keys;
	iter_data.values = &values;
	auto status = H5Aiterate2(object_id, H5_INDEX_NAME, H5_ITER_INC, &idx, H5TreeAllAttributesCallback, &iter_data);
	if (status < 0) {
		if (iter_data.error) {
			throw IOException(iter_data.error_message);
		}
		throw IOException("Failed to iterate attributes");
	}

	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARIANT(), std::move(keys), std::move(values));
}

static void H5TreePopulateAllAttributesValue(H5TreeProjectedAttributeValue &target, hid_t object_id) {
	target.present = true;
	target.value = H5ReadAllAttributesMapValue(object_id);
}

std::string H5TreeNormalizeObjectPath(std::string object_path) {
	if (object_path.empty()) {
		return "/";
	}
	return object_path;
}

H5TreeProjectedAttributeSpec H5TreeParseProjectedAttributeSpec(const Value &input, const std::string &function_name) {
	std::optional<std::string> alias_name;
	auto current = H5TreeUnwrapAliasSpec(input, alias_name);
	if (!H5TreeIsTaggedStructType(current.type())) {
		throw InvalidInputException(H5TreeProjectedAttributeUsage(function_name));
	}
	auto &children = StructValue::GetChildren(current);
	if (children.size() != 3) {
		throw InvalidInputException(H5TreeProjectedAttributeUsage(function_name));
	}
	auto tag = children[0].GetValue<string>();
	if (tag != "__attr__" && tag != "__attr_all__") {
		throw InvalidInputException(H5TreeProjectedAttributeUsage(function_name));
	}
	if (tag == "__attr__" && children[1].IsNull()) {
		throw InvalidInputException("h5_attr name must not be NULL");
	}
	if (children[2].type().id() == LogicalTypeId::SQLNULL) {
		throw InvalidInputException(
		    "h5_attr default_value must have a concrete type; use an explicit cast such as NULL::VARCHAR");
	}

	H5TreeProjectedAttributeSpec spec;
	spec.all_attributes = tag == "__attr_all__";
	if (!spec.all_attributes) {
		spec.attribute_name = children[1].GetValue<string>();
	}
	spec.output_column_name = alias_name ? *alias_name : (spec.all_attributes ? "h5_attr" : spec.attribute_name);
	spec.default_value = children[2];
	spec.output_type = children[2].type();
	return spec;
}

void H5TreeBindProjectedAttributes(const std::string &function_name, const vector<Value> &inputs, idx_t start_idx,
                                   vector<string> &names, vector<LogicalType> &return_types,
                                   vector<H5TreeProjectedAttributeSpec> &projected_attributes) {
	for (idx_t i = start_idx; i < inputs.size(); i++) {
		auto spec = H5TreeParseProjectedAttributeSpec(inputs[i], function_name);
		names.push_back(spec.output_column_name);
		return_types.push_back(spec.output_type);
		projected_attributes.push_back(std::move(spec));
	}
}

std::optional<std::string> H5TreeTypeName(H5TreeEntryType type) {
	switch (type) {
	case H5TreeEntryType::GROUP:
		return std::string("group");
	case H5TreeEntryType::DATASET:
		return std::string("dataset");
	case H5TreeEntryType::DATATYPE:
		return std::string("datatype");
	case H5TreeEntryType::LINK:
		return std::string("link");
	case H5TreeEntryType::EXTERNAL:
		return std::string("external");
	case H5TreeEntryType::UNKNOWN:
		return std::nullopt;
	default:
		throw InternalException("Unknown h5_tree type");
	}
}

static bool H5TreeCanHaveProjectedAttributes(H5TreeEntryType type) {
	return type == H5TreeEntryType::GROUP || type == H5TreeEntryType::DATASET || type == H5TreeEntryType::DATATYPE;
}

H5TreeReadOptions H5TreeReadAll(idx_t projected_attribute_count) {
	H5TreeReadOptions result;
	result.read_type = true;
	result.read_dtype = true;
	result.read_shape = true;
	result.projected_attribute_ids.reserve(projected_attribute_count);
	for (idx_t i = 0; i < projected_attribute_count; i++) {
		result.projected_attribute_ids.push_back(i);
	}
	return result;
}

H5TreeFileReader::H5TreeFileReader(ClientContext &context_p, const std::string &filename_p, bool swmr,
                                   const vector<H5TreeProjectedAttributeSpec> &projected_attributes_p,
                                   H5TreeReadOptions read_options_p)
    : filename(filename_p), projected_attributes(projected_attributes_p), read_options(std::move(read_options_p)) {
	H5ErrorSuppressor suppress;
	file = H5FileHandle(&context_p, filename.c_str(), H5F_ACC_RDONLY, swmr);
	if (!file.is_valid()) {
		throw IOException(FormatRemoteFileError("Failed to open HDF5 file", filename));
	}
}

H5TreeObjectIdentity H5TreeFileReader::GetRootIdentity() {
	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	H5ErrorSuppressor suppress;
	H5O_info2_t root_info;
	if (H5Oget_info3(file, &root_info, H5O_INFO_BASIC) < 0) {
		throw IOException(FormatRemoteFileError("Failed to inspect HDF5 root object", filename));
	}
	return H5TreeIdentityFromObjectInfo(root_info);
}

bool H5TreeFileReader::SameObject(const H5TreeObjectIdentity &lhs, const H5TreeObjectIdentity &rhs) const {
	if (lhs.fileno != rhs.fileno) {
		return false;
	}
	int comparison;
	if (H5Otoken_cmp(file, &lhs.token, &rhs.token, &comparison) < 0) {
		throw IOException(FormatRemoteFileError("Failed to compare HDF5 object identities", filename));
	}
	return comparison == 0;
}

H5ObjectHandle H5TreeFileReader::OpenObject(const std::string &path, hid_t parent_loc, const char *link_name) {
	if (parent_loc >= 0 && link_name) {
		return H5ObjectHandle(parent_loc, link_name);
	}
	return H5ObjectHandle(file, path.c_str());
}

H5TreeEntryType H5TreeFileReader::EntryTypeFromObjectInfo(const H5O_info2_t &info) {
	switch (info.type) {
	case H5O_TYPE_GROUP:
		return H5TreeEntryType::GROUP;
	case H5O_TYPE_DATASET:
		return H5TreeEntryType::DATASET;
	case H5O_TYPE_NAMED_DATATYPE:
		return H5TreeEntryType::DATATYPE;
	default:
		return H5TreeEntryType::UNKNOWN;
	}
}

H5TreeResolvedEntry H5TreeFileReader::ResolveEntry(const H5L_info2_t &link_info, hid_t parent_loc,
                                                   const char *link_name) {
	H5TreeResolvedEntry result;
	if (link_info.type == H5L_TYPE_EXTERNAL) {
		result.type_kind = H5TreeEntryType::EXTERNAL;
		return result;
	}

	H5O_info2_t info;
	if (H5Oget_info_by_name3(parent_loc, link_name, &info, H5O_INFO_BASIC, H5P_DEFAULT) < 0) {
		result.type_kind = H5TreeEntryType::LINK;
		return result;
	}
	result.identity = H5TreeIdentityFromObjectInfo(info);
	result.type_kind = EntryTypeFromObjectInfo(info);
	return result;
}

void H5TreeFileReader::PopulateRowMetadataAndAttributes(H5TreeRow &row, H5TreeEntryType type_kind,
                                                        const std::string &path, hid_t parent_loc,
                                                        const char *link_name) {
	auto need_dataset_metadata =
	    type_kind == H5TreeEntryType::DATASET && (read_options.read_dtype || read_options.read_shape);
	auto need_projected_attributes =
	    !read_options.projected_attribute_ids.empty() && H5TreeCanHaveProjectedAttributes(type_kind);
	if (!need_dataset_metadata && !need_projected_attributes) {
		return;
	}

	H5ErrorSuppressor suppress;
	auto object = OpenObject(path, parent_loc, link_name);
	if (!object.is_valid()) {
		throw IOException(FormatHDF5ObjectError("Failed to open object during tree traversal", filename, path));
	}

	if (need_dataset_metadata && read_options.read_dtype) {
		hid_t type_id = H5Dget_type(object);
		if (type_id < 0) {
			throw IOException(
			    FormatHDF5ObjectError("Failed to get dataset type during tree traversal", filename, path));
		}
		H5TypeHandle type = H5TypeHandle::TakeOwnershipOf(type_id);
		row.dtype = H5TypeToString(type);
	}
	if (need_dataset_metadata && read_options.read_shape) {
		row.shape = H5GetShape(object);
	}

	if (need_projected_attributes) {
		for (auto i : read_options.projected_attribute_ids) {
			D_ASSERT(i < projected_attributes.size());
			D_ASSERT(i < row.projected_values.size());
			try {
				if (projected_attributes[i].all_attributes) {
					H5TreePopulateAllAttributesValue(row.projected_values[i], object);
				} else {
					H5TreePopulateProjectedAttributeValue(row.projected_values[i], object, projected_attributes[i]);
				}
			} catch (const std::exception &ex) {
				throw IOException(FormatHDF5ObjectContextError(H5NormalizeExceptionMessage(ex.what()), filename, path));
			}
		}
	}
}

struct H5TreeListIterData {
	H5TreeFileReader *reader;
	std::string group_path;
	std::vector<H5TreeNamedRow> *rows;
	idx_t max_rows = 0;
	bool error = false;
	std::string error_message;
};

static std::string H5TreeJoinChildPath(const std::string &group_path, const char *name) {
	if (group_path == "/") {
		return "/" + std::string(name);
	}
	return group_path + "/" + name;
}

static void H5TreeValidateListGroupInternal(H5TreeFileReader &reader, const std::string &group_path,
                                            hsize_t *link_count) {
	H5ObjectHandle group(reader.GetFileHandle(), group_path.c_str());
	if (!group.is_valid()) {
		throw IOException(FormatHDF5ObjectError("Failed to open object", reader.GetFilename(), group_path));
	}

	H5O_info2_t group_info;
	if (H5Oget_info3(group, &group_info, H5O_INFO_BASIC) < 0) {
		throw IOException(FormatHDF5ObjectError("Failed to inspect object", reader.GetFilename(), group_path));
	}
	if (group_info.type != H5O_TYPE_GROUP) {
		throw IOException(FormatHDF5ObjectError("Object is not a group", reader.GetFilename(), group_path));
	}
	H5G_info_t group_meta;
	if (link_count && H5Gget_info(group, &group_meta) >= 0) {
		*link_count = group_meta.nlinks;
	}
}

void H5TreeValidateListGroup(H5TreeFileReader &reader, const std::string &group_path) {
	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	H5ErrorSuppressor suppress;
	H5TreeValidateListGroupInternal(reader, group_path, nullptr);
}

static herr_t H5TreeListCallback(hid_t parent_loc, const char *name, const H5L_info2_t *info, void *op_data) {
	auto &data = *reinterpret_cast<H5TreeListIterData *>(op_data);
	try {
		auto path = H5TreeJoinChildPath(data.group_path, name);
		H5TreeNamedRow named_row;
		named_row.name = name;
		named_row.row.path = path;
		if (data.reader->NeedsEntryResolution()) {
			auto resolved = data.reader->ResolveEntry(*info, parent_loc, name);
			if (data.reader->ReadsType()) {
				named_row.row.type = H5TreeTypeName(resolved.type_kind);
			}
			data.reader->InitializeProjectedValues(named_row.row);
			if (resolved.identity) {
				data.reader->PopulateRowMetadataAndAttributes(named_row.row, resolved.type_kind, path, parent_loc,
				                                              name);
			}
		}
		data.rows->push_back(std::move(named_row));
		return data.max_rows > 0 && data.rows->size() >= data.max_rows ? 1 : 0;
	} catch (const std::exception &ex) {
		data.error = true;
		data.error_message = H5NormalizeExceptionMessage(ex.what());
		return -1;
	}
}

static bool H5TreeListEntriesInternal(H5TreeFileReader &reader, const std::string &group_path, hsize_t &idx,
                                      idx_t max_rows, std::vector<H5TreeNamedRow> &rows) {
	H5TreeListIterData iter_data;
	iter_data.reader = &reader;
	iter_data.group_path = group_path;
	iter_data.rows = &rows;
	iter_data.max_rows = max_rows;
	auto status = H5Literate_by_name2(reader.GetFileHandle(), group_path.c_str(), H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
	                                  H5TreeListCallback, &iter_data, H5P_DEFAULT);
	if (status < 0) {
		if (iter_data.error) {
			throw IOException(AppendRemoteError(iter_data.error_message, reader.GetFilename()));
		}
		throw IOException(FormatHDF5ObjectError("Failed to list group entries", reader.GetFilename(), group_path));
	}
	if (status > 0) {
		// A positive callback result means that the output batch is full, but it does not say whether the
		// callback stopped on the group's final link. Resuming H5Literate_by_name2 at idx == nlinks is an
		// HDF5 error, so explicitly detect an exact-full final batch.
		H5G_info_t group_info;
		if (H5Gget_info_by_name(reader.GetFileHandle(), group_path.c_str(), &group_info, H5P_DEFAULT) < 0) {
			throw IOException(
			    FormatHDF5ObjectError("Failed to inspect group during listing", reader.GetFilename(), group_path));
		}
		return idx >= group_info.nlinks;
	}
	return true;
}

void H5TreeListImmediateEntries(H5TreeFileReader &reader, const std::string &group_path,
                                std::vector<H5TreeNamedRow> &rows) {
	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	H5ErrorSuppressor suppress;

	hsize_t link_count = 0;
	H5TreeValidateListGroupInternal(reader, group_path, &link_count);
	rows.reserve(rows.size() + link_count);

	hsize_t idx = 0;
	H5TreeListEntriesInternal(reader, group_path, idx, 0, rows);
}

bool H5TreeListEntriesBatch(H5TreeFileReader &reader, const std::string &group_path, hsize_t &idx, idx_t max_rows,
                            std::vector<H5TreeNamedRow> &rows) {
	D_ASSERT(max_rows > 0);
	rows.clear();
	rows.reserve(max_rows);

	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	H5ErrorSuppressor suppress;
	return H5TreeListEntriesInternal(reader, group_path, idx, max_rows, rows);
}

} // namespace duckdb
