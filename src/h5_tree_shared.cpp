#include "h5_tree_shared.hpp"
#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include <cstring>

namespace duckdb {

static bool H5TreeIsAliasStructType(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	return children.size() == 3 && children[0].second == LogicalType::VARCHAR;
}

static bool H5TreeIsAttrStructType(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	return children.size() == 3 && children[0].second == LogicalType::VARCHAR;
}

static Value H5TreeUnwrapAliasSpec(const Value &input, std::optional<std::string> &alias_name) {
	Value current = input;
	while (H5TreeIsAliasStructType(current.type())) {
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
	if (!H5TreeIsAttrStructType(current.type())) {
		return false;
	}
	auto &children = StructValue::GetChildren(current);
	return children.size() == 3 && children[0].GetValue<string>() == "__attr__";
}

static H5TreeObjectIdentity H5TreeIdentityFromToken(unsigned long fileno, const H5O_token_t &token) {
	H5TreeObjectIdentity identity;
	identity.fileno = fileno;
	std::memcpy(identity.token.data(), &token, sizeof(H5O_token_t));
	return identity;
}

static H5TreeObjectIdentity H5TreeIdentityFromObjectInfo(const H5O_info2_t &info) {
	return H5TreeIdentityFromToken(info.fileno, info.token);
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

static void H5TreeWriteShapeRow(Vector &shape_vector, idx_t row_idx, const H5TreeRow &row, idx_t &shape_offset,
                                uint64_t *shape_data) {
	auto entries = ListVector::GetData(shape_vector);
	auto &validity = FlatVector::Validity(shape_vector);
	if (!row.has_shape) {
		validity.SetInvalid(row_idx);
		entries[row_idx].offset = 0;
		entries[row_idx].length = 0;
		return;
	}
	validity.SetValid(row_idx);
	entries[row_idx].offset = shape_offset;
	entries[row_idx].length = row.shape.size();
	for (auto dim : row.shape) {
		shape_data[shape_offset++] = static_cast<uint64_t>(dim);
	}
}

static void H5TreePopulateProjectedAttributeValue(H5TreeProjectedAttributeValue &target, hid_t object_id,
                                                  const H5TreeProjectedAttributeSpec &spec) {
	auto exists = H5Aexists(object_id, spec.attribute_name.c_str());
	if (exists < 0) {
		throw IOException("Failed to inspect attribute: " + spec.attribute_name);
	}
	if (exists == 0) {
		target.present = false;
		return;
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
	Value cast_value;
	string error_message;
	if (!value.DefaultTryCastAs(spec.output_type, cast_value, &error_message, false)) {
		throw IOException("Attribute '" + spec.attribute_name + "' contains values that cannot be cast to " +
		                  spec.output_type.ToString());
	}
	target.present = true;
	target.value = std::move(cast_value);
}

std::string H5TreeNormalizeObjectPath(std::string object_path) {
	if (object_path.empty()) {
		return "/";
	}
	return object_path;
}

std::string H5TreeNormalizeExceptionMessage(const std::string &message) {
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

H5TreeProjectedAttributeSpec H5TreeParseProjectedAttributeSpec(const Value &input, const std::string &function_name) {
	std::optional<std::string> alias_name;
	auto current = H5TreeUnwrapAliasSpec(input, alias_name);
	if (!H5TreeIsAttrStructType(current.type())) {
		throw InvalidInputException(function_name + " projected attribute arguments must be h5_attr(name) or "
		                                            "h5_attr(name, default_value) or h5_alias(alias, h5_attr(...))");
	}
	auto &children = StructValue::GetChildren(current);
	if (children.size() != 3 || children[0].GetValue<string>() != "__attr__") {
		throw InvalidInputException(function_name + " projected attribute arguments must be h5_attr(name) or "
		                                            "h5_attr(name, default_value) or h5_alias(alias, h5_attr(...))");
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

bool H5TreeCanHaveProjectedAttributes(H5TreeEntryType type) {
	return type == H5TreeEntryType::GROUP || type == H5TreeEntryType::DATASET || type == H5TreeEntryType::DATATYPE;
}

void H5TreeWriteRow(const H5TreeRow &row, const vector<H5TreeProjectedAttributeSpec> &projected_attributes,
                    DataChunk &chunk, idx_t row_idx, idx_t &shape_offset, uint64_t *shape_data) {
	FlatVector::GetData<string_t>(chunk.data[0])[row_idx] = StringVector::AddString(chunk.data[0], row.path);
	H5TreeWriteOptionalString(chunk.data[1], row_idx, row.type);
	H5TreeWriteOptionalString(chunk.data[2], row_idx, row.dtype);
	H5TreeWriteShapeRow(chunk.data[3], row_idx, row, shape_offset, shape_data);
	for (idx_t attr_idx = 0; attr_idx < projected_attributes.size(); attr_idx++) {
		auto &vector = chunk.data[4 + attr_idx];
		const auto &projected = row.projected_values[attr_idx];
		if (projected.present) {
			vector.SetValue(row_idx, projected.value);
		} else {
			vector.SetValue(row_idx, projected_attributes[attr_idx].default_value);
		}
	}
}

H5TreeFileReader::H5TreeFileReader(ClientContext &context_p, const std::string &filename_p, bool swmr,
                                   const vector<H5TreeProjectedAttributeSpec> &projected_attributes_p)
    : context(context_p), filename(filename_p), projected_attributes(projected_attributes_p) {
	H5ErrorSuppressor suppress;
	file = H5FileHandle(&context, filename.c_str(), H5F_ACC_RDONLY, swmr);
	if (!file.is_valid()) {
		throw IOException(FormatRemoteFileError("Failed to open HDF5 file", filename));
	}
	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	InitializeRootIdentity();
}

void H5TreeFileReader::InitializeRootIdentity() {
	H5O_info2_t root_info;
	if (H5Oget_info3(file, &root_info, H5O_INFO_BASIC) < 0) {
		throw IOException(FormatRemoteFileError("Failed to inspect HDF5 root object", filename));
	}
	root_identity = H5TreeIdentityFromObjectInfo(root_info);
}

H5ObjectHandle H5TreeFileReader::OpenObjectByIdentity(const H5TreeObjectIdentity &identity, const std::string &path) {
	if (identity.fileno == root_identity.fileno) {
		H5O_token_t token;
		std::memcpy(&token, identity.token.data(), sizeof(H5O_token_t));
		auto object_id = H5Oopen_by_token(file, token);
		if (object_id >= 0) {
			return H5ObjectHandle::TakeOwnershipOf(object_id);
		}
	}
	return H5ObjectHandle(file, path.c_str());
}

H5ObjectHandle H5TreeFileReader::OpenObject(const H5TreeObjectIdentity &identity, const std::string &path,
                                            hid_t parent_loc, const char *link_name) {
	if (parent_loc >= 0 && link_name) {
		return H5ObjectHandle(parent_loc, link_name);
	}
	return OpenObjectByIdentity(identity, path);
}

bool H5TreeFileReader::ResolveObjectInfo(H5O_info2_t &info, const std::optional<H5TreeObjectIdentity> &identity,
                                         const std::string &path, hid_t parent_loc, const char *link_name) {
	if (parent_loc >= 0 && link_name) {
		return H5Oget_info_by_name3(parent_loc, link_name, &info, H5O_INFO_BASIC, H5P_DEFAULT) >= 0;
	}
	if (identity) {
		auto object = OpenObjectByIdentity(*identity, path);
		return object.is_valid() && H5Oget_info3(object, &info, H5O_INFO_BASIC) >= 0;
	}
	return H5Oget_info_by_name3(file, path.c_str(), &info, H5O_INFO_BASIC, H5P_DEFAULT) >= 0;
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

H5TreeResolvedEntry H5TreeFileReader::ResolveEntry(const std::string &path, const H5L_info2_t &link_info,
                                                   hid_t parent_loc, const char *link_name) {
	H5TreeResolvedEntry result;
	result.is_soft_link = link_info.type == H5L_TYPE_SOFT;

	if (link_info.type == H5L_TYPE_EXTERNAL) {
		result.type_kind = H5TreeEntryType::EXTERNAL;
		return result;
	}

	if (link_info.type == H5L_TYPE_HARD) {
		result.identity = H5TreeIdentityFromToken(root_identity.fileno, link_info.u.token);
	}

	H5O_info2_t info;
	if (!ResolveObjectInfo(info, result.identity, path, parent_loc, link_name)) {
		result.type_kind = H5TreeEntryType::LINK;
		result.identity.reset();
		return result;
	}
	result.identity = H5TreeIdentityFromObjectInfo(info);
	result.type_kind = EntryTypeFromObjectInfo(info);

	if (result.type_kind == H5TreeEntryType::GROUP && result.identity) {
		result.traversable_group = true;
	}

	return result;
}

void H5TreeFileReader::PopulateRowMetadataAndAttributes(H5TreeRow &row, H5TreeEntryType type_kind,
                                                        const H5TreeObjectIdentity &identity, const std::string &path,
                                                        hid_t parent_loc, const char *link_name) {
	auto need_dataset_metadata = type_kind == H5TreeEntryType::DATASET;
	auto need_projected_attributes = !projected_attributes.empty() && H5TreeCanHaveProjectedAttributes(type_kind);
	if (!need_dataset_metadata && !need_projected_attributes) {
		return;
	}

	H5ErrorSuppressor suppress;
	auto object = OpenObject(identity, path, parent_loc, link_name);
	if (!object.is_valid()) {
		throw IOException("Failed to open object during tree traversal: " + path);
	}

	if (need_dataset_metadata) {
		hid_t type_id = H5Dget_type(object);
		if (type_id < 0) {
			throw IOException("Failed to get dataset type during tree traversal: " + path);
		}
		H5TypeHandle type = H5TypeHandle::TakeOwnershipOf(type_id);
		row.dtype = H5TypeToString(type);
		row.shape = H5GetShape(object);
		row.has_shape = true;
	}

	if (need_projected_attributes) {
		for (idx_t i = 0; i < projected_attributes.size(); i++) {
			H5TreePopulateProjectedAttributeValue(row.projected_values[i], object, projected_attributes[i]);
		}
	}
}

struct H5TreeListIterData {
	H5TreeFileReader *reader;
	std::string group_path;
	std::vector<H5TreeNamedRow> *rows;
	bool error = false;
	std::string error_message;
};

static std::string H5TreeJoinChildPath(const std::string &group_path, const char *name) {
	if (group_path == "/") {
		return "/" + std::string(name);
	}
	return group_path + "/" + name;
}

void H5TreeListImmediateEntries(H5TreeFileReader &reader, const std::string &group_path,
                                std::vector<H5TreeNamedRow> &rows) {
	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	H5ErrorSuppressor suppress;

	H5ObjectHandle group(reader.GetFileHandle(), group_path.c_str());
	if (!group.is_valid()) {
		throw IOException(AppendRemoteError("Failed to open object: " + group_path, reader.GetFilename()));
	}

	H5O_info2_t group_info;
	if (H5Oget_info3(group, &group_info, H5O_INFO_BASIC) < 0) {
		throw IOException(AppendRemoteError("Failed to inspect object: " + group_path, reader.GetFilename()));
	}
	if (group_info.type != H5O_TYPE_GROUP) {
		throw IOException(AppendRemoteError("Object is not a group: " + group_path, reader.GetFilename()));
	}
	H5G_info_t group_meta;
	if (H5Gget_info(group, &group_meta) >= 0) {
		rows.reserve(rows.size() + group_meta.nlinks);
	}

	H5TreeListIterData iter_data;
	iter_data.reader = &reader;
	iter_data.group_path = group_path;
	iter_data.rows = &rows;

	hsize_t idx = 0;
	auto callback = [](hid_t parent_loc, const char *name, const H5L_info2_t *info, void *op_data) -> herr_t {
		auto &data = *reinterpret_cast<H5TreeListIterData *>(op_data);
		try {
			auto path = H5TreeJoinChildPath(data.group_path, name);
			auto resolved = data.reader->ResolveEntry(path, *info, parent_loc, name);
			H5TreeNamedRow named_row;
			named_row.name = name;
			named_row.row.path = path;
			named_row.row.type = H5TreeTypeName(resolved.type_kind);
			named_row.row.projected_values.resize(data.reader->GetProjectedAttributes().size());
			if (resolved.identity) {
				data.reader->PopulateRowMetadataAndAttributes(named_row.row, resolved.type_kind, *resolved.identity,
				                                              path, parent_loc, name);
			}
			data.rows->push_back(std::move(named_row));
			return 0;
		} catch (const std::exception &ex) {
			data.error = true;
			data.error_message = ex.what();
			return -1;
		}
	};
	auto status = H5Literate_by_name2(reader.GetFileHandle(), group_path.c_str(), H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
	                                  callback, &iter_data, H5P_DEFAULT);
	if (status < 0) {
		if (iter_data.error) {
			throw IOException(AppendRemoteError(iter_data.error_message, reader.GetFilename()));
		}
		throw IOException(AppendRemoteError("Failed to list group entries: " + group_path, reader.GetFilename()));
	}
}

} // namespace duckdb
