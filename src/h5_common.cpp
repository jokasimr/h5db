#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/common/exception.hpp"
#include <vector>
#include <string>
#include <mutex>

namespace duckdb {

// Global mutex for all HDF5 operations (definition)
std::recursive_mutex hdf5_global_mutex;

// Convert HDF5 type to string representation
std::string H5TypeToString(hid_t type_id) {
	H5T_class_t type_class = H5Tget_class(type_id);
	size_t size = H5Tget_size(type_id);
	H5T_sign_t sign = H5T_SGN_ERROR;

	switch (type_class) {
	case H5T_INTEGER:
		sign = H5Tget_sign(type_id);
		if (sign == H5T_SGN_NONE) {
			// Unsigned integer
			switch (size) {
			case 1:
				return "uint8";
			case 2:
				return "uint16";
			case 4:
				return "uint32";
			case 8:
				return "uint64";
			default:
				return "uint" + std::to_string(size * 8);
			}
		} else {
			// Signed integer
			switch (size) {
			case 1:
				return "int8";
			case 2:
				return "int16";
			case 4:
				return "int32";
			case 8:
				return "int64";
			default:
				return "int" + std::to_string(size * 8);
			}
		}
	case H5T_FLOAT:
		switch (size) {
		case 2:
			return "float16";
		case 4:
			return "float32";
		case 8:
			return "float64";
		default:
			return "float" + std::to_string(size * 8);
		}
	case H5T_STRING:
		return "string";
	case H5T_COMPOUND:
		return "compound";
	case H5T_ENUM:
		return "enum";
	case H5T_ARRAY:
		return "array";
	default:
		return "unknown";
	}
}

// Get dataset shape as string (e.g., "(10,)" or "(5, 4, 3)")
std::string H5GetShapeString(hid_t dataset_id) {
	H5DataspaceHandle space(dataset_id);
	if (!space.is_valid()) {
		return "()";
	}

	int ndims = H5Sget_simple_extent_ndims(space);
	if (ndims <= 0) {
		return "()";
	}

	std::vector<hsize_t> dims(ndims);
	H5Sget_simple_extent_dims(space, dims.data(), nullptr);

	std::string result = "(";
	for (int i = 0; i < ndims; i++) {
		if (i > 0)
			result += ", ";
		result += std::to_string(dims[i]);
	}
	result += ")";

	return result;
}

// Map HDF5 type to DuckDB LogicalType
LogicalType H5TypeToDuckDBType(hid_t type_id) {
	H5T_class_t type_class = H5Tget_class(type_id);
	size_t size = H5Tget_size(type_id);

	switch (type_class) {
	case H5T_INTEGER: {
		H5T_sign_t sign = H5Tget_sign(type_id);
		if (sign == H5T_SGN_NONE) {
			// Unsigned integer
			switch (size) {
			case 1:
				return LogicalType::UTINYINT;
			case 2:
				return LogicalType::USMALLINT;
			case 4:
				return LogicalType::UINTEGER;
			case 8:
				return LogicalType::UBIGINT;
			default:
				throw IOException("Unsupported unsigned integer size: " + std::to_string(size));
			}
		} else {
			// Signed integer
			switch (size) {
			case 1:
				return LogicalType::TINYINT;
			case 2:
				return LogicalType::SMALLINT;
			case 4:
				return LogicalType::INTEGER;
			case 8:
				return LogicalType::BIGINT;
			default:
				throw IOException("Unsupported signed integer size: " + std::to_string(size));
			}
		}
	}
	case H5T_FLOAT:
		switch (size) {
		case 4:
			return LogicalType::FLOAT;
		case 8:
			return LogicalType::DOUBLE;
		default:
			throw IOException("Unsupported float size: " + std::to_string(size));
		}
	case H5T_STRING:
		return LogicalType::VARCHAR;
	default:
		throw IOException("Unsupported HDF5 type class: " + std::to_string(type_class));
	}
}

// Convert HDF5 attribute type (including arrays) to DuckDB type
LogicalType H5AttributeTypeToDuckDBType(hid_t type_id) {
	H5T_class_t type_class = H5Tget_class(type_id);

	if (type_class == H5T_ARRAY) {
		hid_t base_type_id = H5Tget_super(type_id);
		if (base_type_id < 0) {
			throw IOException("Failed to get array base type");
		}
		H5TypeHandle base_type = H5TypeHandle::TakeOwnershipOf(base_type_id);

		int ndims = H5Tget_array_ndims(type_id);
		if (ndims < 0) {
			throw IOException("Failed to get array dimensions");
		}
		if (ndims != 1) {
			throw IOException("Only 1D array attributes are supported, found " + std::to_string(ndims) + "D array");
		}

		hsize_t dims[1];
		if (H5Tget_array_dims2(type_id, dims) < 0) {
			throw IOException("Failed to get array dimensions");
		}

		LogicalType element_type = H5TypeToDuckDBType(base_type);
		return LogicalType::ARRAY(element_type, dims[0]);
	}

	return H5TypeToDuckDBType(type_id);
}

} // namespace duckdb
