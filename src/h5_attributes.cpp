#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include <vector>
#include <string>

namespace duckdb {

//===--------------------------------------------------------------------===//
// h5_attributes - Read attributes from HDF5 datasets/groups
//===--------------------------------------------------------------------===//

// Helper to convert HDF5 attribute type (including arrays) to DuckDB type
static LogicalType H5AttributeTypeToDuckDBType(hid_t type_id) {
	H5T_class_t type_class = H5Tget_class(type_id);

	if (type_class == H5T_ARRAY) {
		// Get the base type of the array - RAII handles cleanup
		hid_t base_type_id = H5Tget_super(type_id);
		if (base_type_id < 0) {
			throw IOException("Failed to get array base type");
		}
		H5TypeHandle base_type(base_type_id);

		// Get array dimensions
		int ndims = H5Tget_array_ndims(type_id);
		if (ndims < 0) {
			throw IOException("Failed to get array dimensions");
		}
		if (ndims != 1) {
			throw IOException("Only 1D array attributes are supported, found " + std::to_string(ndims) + "D array");
		}

		// Get the size of the array
		hsize_t dims[1];
		if (H5Tget_array_dims2(type_id, dims) < 0) {
			throw IOException("Failed to get array dimensions");
		}

		// Convert base type to DuckDB type
		LogicalType element_type = H5TypeToDuckDBType(base_type);

		// Return ARRAY type with fixed size
		return LogicalType::ARRAY(element_type, dims[0]);
	}

	// For non-array types, use the existing converter
	return H5TypeToDuckDBType(type_id);
}

struct AttributeInfo {
	std::string name;
	LogicalType type;
	H5TypeHandle h5_type;
};

struct H5AttributesBindData : public TableFunctionData {
	std::string filename;
	std::string object_path;
	std::vector<AttributeInfo> attributes;
};

struct H5AttributesGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

// Callback for H5Aiterate2 to collect attribute names
static herr_t attr_info_callback(hid_t location_id, const char *attr_name, const H5A_info_t *ainfo, void *op_data) {
	auto &attributes = *reinterpret_cast<std::vector<AttributeInfo> *>(op_data);

	// Open the attribute - RAII handles cleanup
	H5AttributeHandle attr(location_id, attr_name);
	if (!attr.is_valid()) {
		throw IOException("Failed to open attribute: " + std::string(attr_name));
	}

	// Get the attribute's datatype - RAII handles cleanup
	hid_t type_id = H5Aget_type(attr);
	if (type_id < 0) {
		throw IOException("Failed to get type for attribute: " + std::string(attr_name));
	}
	H5TypeHandle type(type_id);

	// Get the dataspace to check if it's scalar or simple - RAII handles cleanup
	hid_t space_id = H5Aget_space(attr);
	if (space_id < 0) {
		throw IOException("Failed to get dataspace for attribute: " + std::string(attr_name));
	}
	H5DataspaceHandle space = H5DataspaceHandle::TakeOwnershipOf(space_id);

	H5S_class_t space_class = H5Sget_simple_extent_type(space);

	// Check dataspace dimensions
	int ndims = H5Sget_simple_extent_ndims(space);
	hsize_t dims[H5S_MAX_RANK];
	if (ndims > 0) {
		H5Sget_simple_extent_dims(space, dims, nullptr);
	}

	// We support:
	// 1. Scalar dataspaces (ndims == 0 or space_class == H5S_SCALAR)
	// 2. Simple 1D dataspaces for array attributes
	if (space_class != H5S_SCALAR && space_class != H5S_SIMPLE) {
		throw IOException("Attribute '" + std::string(attr_name) + "' has unsupported dataspace class");
	}

	if (space_class == H5S_SIMPLE && ndims > 1) {
		throw IOException("Attribute '" + std::string(attr_name) +
		                  "' has unsupported multidimensional dataspace (only 1D arrays supported)");
	}

	// Convert to DuckDB type
	LogicalType duckdb_type;
	try {
		// If the dataspace is SIMPLE (1D), create an ARRAY type
		if (space_class == H5S_SIMPLE && ndims == 1) {
			LogicalType element_type = H5TypeToDuckDBType(type);
			duckdb_type = LogicalType::ARRAY(element_type, dims[0]);
		} else {
			// For scalar dataspaces, use the normal converter (which handles H5T_ARRAY types)
			duckdb_type = H5AttributeTypeToDuckDBType(type);
		}
	} catch (const std::exception &e) {
		throw IOException("Attribute '" + std::string(attr_name) + "' has unsupported type: " + std::string(e.what()));
	}

	// Store attribute info
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

	// Get parameters
	result->filename = input.inputs[0].GetValue<string>();
	result->object_path = input.inputs[1].GetValue<string>();

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::mutex> lock(hdf5_global_mutex);

	// Open the HDF5 file - RAII wrappers handle cleanup
	H5ErrorSuppressor suppress_errors;
	H5FileHandle file(result->filename.c_str(), H5F_ACC_RDONLY);
	if (!file.is_valid()) {
		throw IOException("Failed to open HDF5 file: " + result->filename);
	}

	// Open the object (dataset or group)
	H5ObjectHandle obj(file, result->object_path.c_str());
	if (!obj.is_valid()) {
		throw IOException("Failed to open object: " + result->object_path + " in file: " + result->filename);
	}

	// Iterate through attributes
	hsize_t idx = 0;
	herr_t status = H5Aiterate2(obj, H5_INDEX_NAME, H5_ITER_NATIVE, &idx, attr_info_callback, &result->attributes);

	if (status < 0) {
		throw IOException("Failed to iterate attributes for: " + result->object_path);
	}

	// Build the return schema - one column per attribute
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

	// Only return one row
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::mutex> lock(hdf5_global_mutex);

	// Open the file and object - RAII wrappers handle cleanup
	H5ErrorSuppressor suppress_errors;
	H5FileHandle file(bind_data.filename.c_str(), H5F_ACC_RDONLY);
	if (!file.is_valid()) {
		throw IOException("Failed to open HDF5 file: " + bind_data.filename);
	}

	H5ObjectHandle obj(file, bind_data.object_path.c_str());
	if (!obj.is_valid()) {
		throw IOException("Failed to open object: " + bind_data.object_path);
	}

	// Read each attribute and fill the corresponding column
	for (idx_t col_idx = 0; col_idx < bind_data.attributes.size(); col_idx++) {
		const auto &attr_info = bind_data.attributes[col_idx];
		auto &result_vector = output.data[col_idx];

		// Open the attribute - RAII wrapper handles cleanup
		H5AttributeHandle attr(obj, attr_info.name.c_str());
		if (!attr.is_valid()) {
			throw IOException("Failed to open attribute: " + attr_info.name);
		}

		// Read the attribute value based on its type
		if (attr_info.type.id() == LogicalTypeId::ARRAY) {
			// Handle array attributes
			auto array_child_type = ArrayType::GetChildType(attr_info.type);
			auto array_size = ArrayType::GetSize(attr_info.type);

			// Get the child vector where array data is stored
			auto &child_vector = ArrayVector::GetEntry(result_vector);

			// Dispatch on the element type to read the array data directly into child vector
			DispatchOnDuckDBType(array_child_type, [&](auto type_tag) {
				using T = typename decltype(type_tag)::type;

				// Get pointer to child vector data
				auto child_data = FlatVector::GetData<T>(child_vector);

				// Read the array attribute directly into the child vector
				if (H5Aread(attr, attr_info.h5_type.get(), child_data) < 0) {
					throw IOException("Failed to read array attribute: " + attr_info.name);
				}
			});

		} else if (attr_info.type.id() == LogicalTypeId::VARCHAR) {
			// Handle string attributes
			htri_t is_variable = H5Tis_variable_str(attr_info.h5_type.get());

			if (is_variable > 0) {
				// Variable-length string
				char *str_ptr = nullptr;
				if (H5Aread(attr, attr_info.h5_type.get(), &str_ptr) < 0) {
					throw IOException("Failed to read variable-length string attribute: " + attr_info.name);
				}

				if (str_ptr) {
					FlatVector::GetData<string_t>(result_vector)[0] = StringVector::AddString(result_vector, str_ptr);
					// Free HDF5-allocated string
					free(str_ptr);
				} else {
					FlatVector::SetNull(result_vector, 0, true);
				}

			} else {
				// Fixed-length string
				size_t str_len = H5Tget_size(attr_info.h5_type.get());
				std::vector<char> buffer(str_len);

				if (H5Aread(attr, attr_info.h5_type.get(), buffer.data()) < 0) {
					throw IOException("Failed to read fixed-length string attribute: " + attr_info.name);
				}

				// Find actual string length (may be null-terminated or space-padded)
				size_t actual_len = strnlen(buffer.data(), str_len);
				FlatVector::GetData<string_t>(result_vector)[0] =
				    StringVector::AddString(result_vector, buffer.data(), actual_len);
			}

		} else {
			// Handle scalar attributes - use type dispatcher
			DispatchOnDuckDBType(attr_info.type, [&](auto type_tag) {
				using T = typename decltype(type_tag)::type;

				T value;
				if (H5Aread(attr, attr_info.h5_type.get(), &value) < 0) {
					throw IOException("Failed to read attribute: " + attr_info.name);
				}

				// Store the value in the output vector
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

	loader.RegisterFunction(h5_attributes);
}

} // namespace duckdb
