#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/config.hpp"
#include <vector>
#include <string>
#include <mutex>
#include <cstring>

namespace duckdb {

static std::string NormalizeAttributeTypeError(const std::exception &ex) {
	return H5NormalizeExceptionMessage(ex.what());
}

struct H5AttributeTypeResolution {
	bool supported = false;
	LogicalType type;
	std::string unsupported_reason;
};

template <typename T>
static Value CreateDuckDBValue(T value) {
	if constexpr (std::is_same_v<T, uint8_t>) {
		return Value::UTINYINT(value);
	} else if constexpr (std::is_same_v<T, uint16_t>) {
		return Value::USMALLINT(value);
	} else if constexpr (std::is_same_v<T, uint32_t>) {
		return Value::UINTEGER(value);
	} else if constexpr (std::is_same_v<T, uint64_t>) {
		return Value::UBIGINT(value);
	} else {
		return Value(value);
	}
}

// Global mutex for all HDF5 operations (definition)
std::recursive_mutex hdf5_global_mutex;

bool ResolveSwmrOption(ClientContext &context, const named_parameter_map_t &named_parameters) {
	auto it = named_parameters.find("swmr");
	if (it != named_parameters.end()) {
		return it->second.GetValue<bool>();
	}

	Value setting;
	if (context.TryGetCurrentSetting("h5db_swmr_default", setting)) {
		return setting.GetValue<bool>();
	}

	return false;
}

std::string GetRequiredStringArgument(const Value &value, const std::string &function_name,
                                      const std::string &argument_name) {
	if (value.IsNull()) {
		throw InvalidInputException("%s %s must not be NULL", function_name, argument_name);
	}
	return value.GetValue<string>();
}

idx_t ParseBatchSizeSetting(const Value &setting_value) {
	auto input = setting_value.ToString();
	idx_t parsed;
	try {
		parsed = DBConfig::ParseMemoryLimit(input);
	} catch (std::exception &) {
		throw InvalidInputException("Invalid value for h5db_batch_size: %s", input);
	}

	if (parsed == 0 || parsed == DConstants::INVALID_INDEX ||
	    parsed >= static_cast<idx_t>(NumericLimits<int64_t>::Maximum())) {
		throw InvalidInputException("Invalid value for h5db_batch_size: %s", input);
	}
	return parsed;
}

idx_t ResolveBatchSizeOption(ClientContext &context) {
	Value setting;
	if (!context.TryGetCurrentSetting("h5db_batch_size", setting)) {
		return H5DB_DEFAULT_BATCH_SIZE_BYTES;
	}

	auto parsed = ParseBatchSizeSetting(setting);
	return MinValue<idx_t>(parsed, H5DB_MAX_BATCH_SIZE_BYTES);
}

bool IsInterrupted(ClientContext &context) {
	return context.interrupted.load(std::memory_order_relaxed);
}

void ThrowIfInterrupted(ClientContext &context) {
	if (IsInterrupted(context)) {
		throw InterruptException();
	}
}

H5RemoteErrorInfo TakeRemoteErrorInfo(const std::string &filename) {
	if (!H5RemoteVFD::IsRemotePath(filename)) {
		return {};
	}
	return H5RemoteVFD::TakeLastErrorInfo();
}

std::string AppendRemoteError(const std::string &message, const std::string &filename) {
	auto error = TakeRemoteErrorInfo(filename);
	if (error.interrupted) {
		throw InterruptException();
	}
	if (!error.message.empty()) {
		auto remote_message = error.message;
		if (!remote_message.empty() && remote_message[0] == '{') {
			ErrorData error_data(remote_message);
			if (!error_data.RawMessage().empty()) {
				remote_message = error_data.RawMessage();
			}
		}
		return message + " (" + remote_message + ")";
	}
	return message;
}

std::string FormatRemoteFileError(const std::string &prefix, const std::string &filename) {
	return AppendRemoteError(prefix + ": " + filename, filename);
}

std::string H5NormalizeExceptionMessage(const std::string &message) {
	auto json_start = message.find('{');
	if (json_start == string::npos) {
		return message;
	}
	try {
		auto info = StringUtil::ParseJSONMap(message.substr(json_start), true)->Flatten();
		for (const auto &entry : info) {
			if (entry.first == "exception_message") {
				return entry.second;
			}
		}
	} catch (...) {
	}
	return message;
}

static Value H5CreateStringAttributeValue(std::string result, H5StringDecodeMode string_decode_mode) {
	if (!Value::StringIsValid(result)) {
		if (string_decode_mode == H5StringDecodeMode::TEXT_OR_BLOB) {
			return Value::BLOB(const_data_ptr_cast(result.data()), result.size());
		}
		throw ErrorManager::InvalidUnicodeError(result, "value construction");
	}
	return Value(std::move(result));
}

static Value H5CreateStringAttributeValue(std::string result, const char *raw_data, size_t raw_size,
                                          H5StringDecodeMode string_decode_mode) {
	if (!Value::StringIsValid(result)) {
		if (string_decode_mode == H5StringDecodeMode::TEXT_OR_BLOB) {
			return Value::BLOB(const_data_ptr_cast(raw_data), raw_size);
		}
		throw ErrorManager::InvalidUnicodeError(result, "value construction");
	}
	return Value(std::move(result));
}

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

// Get dataset shape as a list of dimensions
std::vector<hsize_t> H5GetShape(hid_t dataset_id) {
	H5DataspaceHandle space(dataset_id);
	if (!space.is_valid()) {
		return {};
	}

	int ndims = H5Sget_simple_extent_ndims(space);
	if (ndims <= 0) {
		return {};
	}

	std::vector<hsize_t> dims(ndims);
	H5Sget_simple_extent_dims(space, dims.data(), nullptr);
	return dims;
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

static H5AttributeTypeResolution H5ResolveAttributeLogicalTypeInternal(hid_t type_id, hid_t space_id) {
	auto space_class = H5Sget_simple_extent_type(space_id);
	auto ndims = H5Sget_simple_extent_ndims(space_id);
	hsize_t dims[H5S_MAX_RANK];
	if (space_class == H5S_NO_CLASS || ndims < 0) {
		throw IOException("Failed to inspect attribute dataspace");
	}
	if (ndims > 0) {
		if (H5Sget_simple_extent_dims(space_id, dims, nullptr) < 0) {
			throw IOException("Failed to inspect attribute dimensions");
		}
	}

	if (space_class != H5S_SCALAR && space_class != H5S_SIMPLE) {
		return {false, LogicalType(), "unsupported dataspace class"};
	}
	if (space_class == H5S_SIMPLE && ndims > 1) {
		return {false, LogicalType(), "unsupported multidimensional dataspace (only 1D arrays supported)"};
	}

	LogicalType duckdb_type;
	if (space_class == H5S_SIMPLE && ndims == 1) {
		try {
			LogicalType element_type = H5TypeToDuckDBType(type_id);
			duckdb_type = LogicalType::ARRAY(element_type, dims[0]);
		} catch (const std::exception &ex) {
			return {false, LogicalType(), "unsupported type: " + NormalizeAttributeTypeError(ex)};
		}
	} else {
		try {
			duckdb_type = H5AttributeTypeToDuckDBType(type_id);
		} catch (const std::exception &ex) {
			return {false, LogicalType(), "unsupported type: " + NormalizeAttributeTypeError(ex)};
		}
	}

	if (duckdb_type.id() == LogicalTypeId::ARRAY &&
	    ArrayType::GetChildType(duckdb_type).id() == LogicalTypeId::VARCHAR) {
		return {false, LogicalType(), "unsupported type: string array attributes are not supported"};
	}

	return {true, duckdb_type, ""};
}

LogicalType H5ResolveAttributeLogicalType(hid_t type_id, hid_t space_id, const std::string &attribute_name) {
	auto resolution = H5ResolveAttributeLogicalTypeInternal(type_id, space_id);
	if (!resolution.supported) {
		throw IOException("Attribute '" + attribute_name + "' has " + resolution.unsupported_reason);
	}
	return resolution.type;
}

bool H5TryResolveAttributeLogicalType(hid_t type_id, hid_t space_id, LogicalType &duckdb_type) {
	auto resolution = H5ResolveAttributeLogicalTypeInternal(type_id, space_id);
	if (!resolution.supported) {
		return false;
	}
	duckdb_type = std::move(resolution.type);
	return true;
}

H5OpenedAttribute H5OpenAttribute(hid_t object_id, const std::string &attribute_name) {
	H5OpenedAttribute result;
	result.attr = H5AttributeHandle(object_id, attribute_name.c_str());
	if (!result.attr.is_valid()) {
		throw IOException("Failed to open attribute: " + attribute_name);
	}

	hid_t type_id = H5Aget_type(result.attr);
	if (type_id < 0) {
		throw IOException("Failed to get type for attribute: " + attribute_name);
	}
	result.type = H5TypeHandle::TakeOwnershipOf(type_id);

	hid_t space_id = H5Aget_space(result.attr);
	if (space_id < 0) {
		throw IOException("Failed to get dataspace for attribute: " + attribute_name);
	}
	result.space = H5DataspaceHandle::TakeOwnershipOf(space_id);
	return result;
}

Value H5ReadAttributeValue(hid_t attr_id, hid_t h5_type_id, const LogicalType &duckdb_type,
                           const std::string &attribute_name, H5StringDecodeMode string_decode_mode) {
	if (duckdb_type.id() == LogicalTypeId::ARRAY) {
		auto &child_type = ArrayType::GetChildType(duckdb_type);
		auto array_size = ArrayType::GetSize(duckdb_type);
		return DispatchOnDuckDBType(child_type, [&](auto type_tag) -> Value {
			using T = typename decltype(type_tag)::type;
			std::vector<Value> values;
			values.reserve(array_size);

			if constexpr (std::is_same_v<T, string>) {
				throw IOException("Attribute '" + attribute_name +
				                  "' has unsupported type: string array attributes are not supported");
			} else {
				std::vector<T> raw_values(array_size);
				H5ErrorSuppressor suppress;
				if (H5Aread(attr_id, h5_type_id, raw_values.data()) < 0) {
					throw IOException("Failed to read array attribute: " + attribute_name);
				}
				for (auto &value : raw_values) {
					values.push_back(CreateDuckDBValue(value));
				}
			}
			return Value::ARRAY(child_type, std::move(values));
		});
	}

	if (duckdb_type.id() == LogicalTypeId::VARCHAR) {
		htri_t is_variable = H5Tis_variable_str(h5_type_id);
		if (is_variable > 0) {
			char *str_ptr = nullptr;
			H5ErrorSuppressor suppress;
			if (H5Aread(attr_id, h5_type_id, &str_ptr) < 0) {
				throw IOException("Failed to read variable-length string attribute: " + attribute_name);
			}
			if (!str_ptr) {
				return Value(LogicalType::VARCHAR);
			}
			std::string result(str_ptr);
			if (H5free_memory(str_ptr) < 0) {
				throw IOException("Failed to reclaim variable-length string attribute: " + attribute_name);
			}
			return H5CreateStringAttributeValue(std::move(result), string_decode_mode);
		}

		size_t str_len = H5Tget_size(h5_type_id);
		std::vector<char> buffer(str_len);
		H5ErrorSuppressor suppress;
		if (H5Aread(attr_id, h5_type_id, buffer.data()) < 0) {
			throw IOException("Failed to read fixed-length string attribute: " + attribute_name);
		}
		size_t actual_len = strnlen(buffer.data(), str_len);
		std::string result(buffer.data(), actual_len);
		return H5CreateStringAttributeValue(std::move(result), buffer.data(), buffer.size(), string_decode_mode);
	}

	return DispatchOnDuckDBType(duckdb_type, [&](auto type_tag) -> Value {
		using T = typename decltype(type_tag)::type;
		T value;
		H5ErrorSuppressor suppress;
		if (H5Aread(attr_id, h5_type_id, &value) < 0) {
			throw IOException("Failed to read attribute: " + attribute_name);
		}
		return CreateDuckDBValue(value);
	});
}

} // namespace duckdb
