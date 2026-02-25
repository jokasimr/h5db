#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/complex_json.hpp"
#include <vector>
#include <string>

namespace duckdb {

//===--------------------------------------------------------------------===//
// h5_attributes - Read attributes from HDF5 datasets/groups
//===--------------------------------------------------------------------===//

struct AttributeInfo {
	std::string name;
	LogicalType type;
	H5TypeHandle h5_type;
};

struct H5AttributesBindData : public TableFunctionData {
	std::string filename;
	std::string object_path;
	std::vector<AttributeInfo> attributes;
	bool swmr = false;
};

struct H5AttributesGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

struct AttrIterData {
	std::vector<AttributeInfo> *attributes;
	bool error = false;
	std::string error_message;
};

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

// Callback for H5Aiterate2 to collect attribute names
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

	H5AttributeHandle attr(location_id, attr_name);
	if (!attr.is_valid()) {
		return fail("Failed to open attribute: " + std::string(attr_name));
	}

	hid_t type_id = H5Aget_type(attr);
	if (type_id < 0) {
		return fail("Failed to get type for attribute: " + std::string(attr_name));
	}
	H5TypeHandle type = H5TypeHandle::TakeOwnershipOf(type_id);

	hid_t space_id = H5Aget_space(attr);
	if (space_id < 0) {
		return fail("Failed to get dataspace for attribute: " + std::string(attr_name));
	}
	H5DataspaceHandle space = H5DataspaceHandle::TakeOwnershipOf(space_id);

	H5S_class_t space_class = H5Sget_simple_extent_type(space);

	int ndims = H5Sget_simple_extent_ndims(space);
	hsize_t dims[H5S_MAX_RANK];
	if (ndims > 0) {
		H5Sget_simple_extent_dims(space, dims, nullptr);
	}

	// Support scalar and simple 1D array attributes.
	if (space_class != H5S_SCALAR && space_class != H5S_SIMPLE) {
		return fail("Attribute '" + std::string(attr_name) + "' has unsupported dataspace class");
	}

	if (space_class == H5S_SIMPLE && ndims > 1) {
		return fail("Attribute '" + std::string(attr_name) +
		            "' has unsupported multidimensional dataspace (only 1D arrays supported)");
	}

	LogicalType duckdb_type;
	// If the dataspace is SIMPLE (1D), create an ARRAY type
	if (space_class == H5S_SIMPLE && ndims == 1) {
		try {
			LogicalType element_type = H5TypeToDuckDBType(type);
			duckdb_type = LogicalType::ARRAY(element_type, dims[0]);
		} catch (const std::exception &e) {
			return fail("Attribute '" + std::string(attr_name) +
			            "' has unsupported type: " + NormalizeExceptionMessage(e.what()));
		}
	} else {
		// For scalar dataspaces, use the normal converter (which handles H5T_ARRAY types)
		try {
			duckdb_type = H5AttributeTypeToDuckDBType(type);
		} catch (const std::exception &e) {
			return fail("Attribute '" + std::string(attr_name) +
			            "' has unsupported type: " + NormalizeExceptionMessage(e.what()));
		}
	}

	AttributeInfo info;
	info.name = attr_name;
	info.type = duckdb_type;
	info.h5_type = std::move(type);

	attributes.push_back(std::move(info));

	return 0; // Continue iteration
}

static unique_ptr<FunctionData> H5AttributesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5AttributesBindData>();

	result->filename = input.inputs[0].GetValue<string>();
	result->object_path = input.inputs[1].GetValue<string>();
	result->swmr = ResolveSwmrOption(context, input.named_parameters);

	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

	H5ErrorSuppressor suppress_errors;
	H5FileHandle file(result->filename.c_str(), H5F_ACC_RDONLY, result->swmr);
	if (!file.is_valid()) {
		throw IOException("Failed to open HDF5 file: " + result->filename);
	}

	H5ObjectHandle obj(file, result->object_path.c_str());
	if (!obj.is_valid()) {
		throw IOException("Failed to open object: " + result->object_path + " in file: " + result->filename);
	}

	hsize_t idx = 0;
	AttrIterData iter_data;
	iter_data.attributes = &result->attributes;
	herr_t status = H5Aiterate2(obj, H5_INDEX_NAME, H5_ITER_NATIVE, &idx, attr_info_callback, &iter_data);

	if (status < 0) {
		if (iter_data.error) {
			throw IOException(iter_data.error_message);
		}
		throw IOException("Failed to iterate attributes for: " + result->object_path);
	}

	if (result->attributes.empty()) {
		throw IOException("Object has no attributes: " + result->object_path);
	}

	for (const auto &attr : result->attributes) {
		names.push_back(attr.name);
		return_types.push_back(attr.type);
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> H5AttributesInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<H5AttributesGlobalState>();
}

static void H5AttributesScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &gstate = input.global_state->Cast<H5AttributesGlobalState>();
	auto &bind_data = input.bind_data->Cast<H5AttributesBindData>();

	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}

	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

	H5ErrorSuppressor suppress_errors;
	H5FileHandle file(bind_data.filename.c_str(), H5F_ACC_RDONLY, bind_data.swmr);
	if (!file.is_valid()) {
		throw IOException("Failed to open HDF5 file: " + bind_data.filename);
	}

	H5ObjectHandle obj(file, bind_data.object_path.c_str());
	if (!obj.is_valid()) {
		throw IOException("Failed to open object: " + bind_data.object_path);
	}

	for (idx_t col_idx = 0; col_idx < bind_data.attributes.size(); col_idx++) {
		const auto &attr_info = bind_data.attributes[col_idx];
		auto &result_vector = output.data[col_idx];

		H5AttributeHandle attr(obj, attr_info.name.c_str());
		if (!attr.is_valid()) {
			throw IOException("Failed to open attribute: " + attr_info.name);
		}

		if (attr_info.type.id() == LogicalTypeId::ARRAY) {
			auto array_child_type = ArrayType::GetChildType(attr_info.type);

			auto &child_vector = ArrayVector::GetEntry(result_vector);

			DispatchOnDuckDBType(array_child_type, [&](auto type_tag) {
				using T = typename decltype(type_tag)::type;

				auto child_data = FlatVector::GetData<T>(child_vector);

				if (H5Aread(attr, attr_info.h5_type.get(), child_data) < 0) {
					throw IOException("Failed to read array attribute: " + attr_info.name);
				}
			});

		} else if (attr_info.type.id() == LogicalTypeId::VARCHAR) {
			htri_t is_variable = H5Tis_variable_str(attr_info.h5_type.get());

			if (is_variable > 0) {
				char *str_ptr = nullptr;
				if (H5Aread(attr, attr_info.h5_type.get(), &str_ptr) < 0) {
					throw IOException("Failed to read variable-length string attribute: " + attr_info.name);
				}

				if (str_ptr) {
					FlatVector::GetData<string_t>(result_vector)[0] = StringVector::AddString(result_vector, str_ptr);
					free(str_ptr);
				} else {
					FlatVector::SetNull(result_vector, 0, true);
				}

			} else {
				size_t str_len = H5Tget_size(attr_info.h5_type.get());
				std::vector<char> buffer(str_len);

				if (H5Aread(attr, attr_info.h5_type.get(), buffer.data()) < 0) {
					throw IOException("Failed to read fixed-length string attribute: " + attr_info.name);
				}

				size_t actual_len = strnlen(buffer.data(), str_len);
				FlatVector::GetData<string_t>(result_vector)[0] =
				    StringVector::AddString(result_vector, buffer.data(), actual_len);
			}

		} else {
			DispatchOnDuckDBType(attr_info.type, [&](auto type_tag) {
				using T = typename decltype(type_tag)::type;

				T value;
				if (H5Aread(attr, attr_info.h5_type.get(), &value) < 0) {
					throw IOException("Failed to read attribute: " + attr_info.name);
				}

				auto data = FlatVector::GetData<T>(result_vector);
				data[0] = value;
			});
		}
	}

	gstate.done = true;
	output.SetCardinality(1);
}

void RegisterH5AttributesFunction(ExtensionLoader &loader) {
	TableFunction h5_attributes("h5_attributes", {LogicalType::VARCHAR, LogicalType::VARCHAR}, H5AttributesScan,
	                            H5AttributesBind, H5AttributesInit);
	h5_attributes.name = "h5_attributes";
	h5_attributes.named_parameters["swmr"] = LogicalType::BOOLEAN;

	loader.RegisterFunction(h5_attributes);
}

} // namespace duckdb
