#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "h5_remote_backend.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/config.hpp"
#include <algorithm>
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

H5FilenameColumnOption ResolveFilenameColumnOption(const named_parameter_map_t &named_parameters) {
	H5FilenameColumnOption result;
	auto it = named_parameters.find("filename");
	if (it == named_parameters.end()) {
		return result;
	}
	auto &value = it->second;
	if (value.IsNull()) {
		throw InvalidInputException("Cannot use NULL as argument for \"filename\"");
	}
	if (value.type() == LogicalType::VARCHAR) {
		result.include = true;
		result.column_name = StringValue::Get(value);
		return result;
	}

	Value boolean_value;
	string error_message;
	if (value.DefaultTryCastAs(LogicalType::BOOLEAN, boolean_value, &error_message)) {
		result.include = BooleanValue::Get(boolean_value);
	}
	return result;
}

H5ExpandedFileList H5ExpandFilePattern(ClientContext &context, const std::string &pattern) {
	if (StringUtil::StartsWith(StringUtil::Lower(pattern), "sftp://")) {
		return ExpandH5SftpFilePattern(context, pattern);
	}

	H5ExpandedFileList result;
	if (!FileSystem::HasGlob(pattern)) {
		result.filenames.push_back(pattern);
		return result;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto expanded = fs.GlobFiles(pattern, FileGlobOptions::DISALLOW_EMPTY);
	result.had_glob = true;
	result.filenames.reserve(expanded.size());
	for (auto &file : expanded) {
		result.filenames.push_back(std::move(file.path));
	}
	std::sort(result.filenames.begin(), result.filenames.end());
	return result;
}

H5ExpandedFileList H5ExpandFilePatterns(ClientContext &context, const Value &input, const std::string &function_name) {
	auto reader = MultiFileReader::CreateDefault(function_name);
	auto patterns = reader->ParsePaths(input);
	if (patterns.empty()) {
		throw IOException("%s needs at least one file to read", function_name);
	}

	H5ExpandedFileList result;
	for (const auto &pattern : patterns) {
		auto expanded = H5ExpandFilePattern(context, pattern);
		result.had_glob = result.had_glob || expanded.had_glob;
		std::move(expanded.filenames.begin(), expanded.filenames.end(), std::back_inserter(result.filenames));
	}
	return result;
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

bool H5StringMatchesCharset(const std::string &value, H5T_cset_t cset) {
	switch (cset) {
	case H5T_CSET_ASCII:
		for (auto byte : value) {
			if (static_cast<unsigned char>(byte) > 0x7F) {
				return false;
			}
		}
		return true;
	case H5T_CSET_UTF8:
		return Value::StringIsValid(value);
	default:
		return Value::StringIsValid(value);
	}
}

std::string H5DecodeFixedLengthString(const char *raw_data, size_t raw_size, H5T_str_t strpad) {
	size_t decoded_size = raw_size;
	switch (strpad) {
	case H5T_STR_NULLTERM: {
		auto null_it = std::find(raw_data, raw_data + raw_size, '\0');
		decoded_size = static_cast<size_t>(null_it - raw_data);
		break;
	}
	case H5T_STR_NULLPAD:
		while (decoded_size > 0 && raw_data[decoded_size - 1] == '\0') {
			decoded_size--;
		}
		break;
	case H5T_STR_SPACEPAD:
		while (decoded_size > 0 && raw_data[decoded_size - 1] == ' ') {
			decoded_size--;
		}
		break;
	default:
		break;
	}
	return std::string(raw_data, decoded_size);
}

static bool H5StringDecodeModePreservesRawBytes(H5StringDecodeMode string_decode_mode) {
	return string_decode_mode == H5StringDecodeMode::TEXT_OR_BLOB;
}

static Value H5CreateDecodedStringValue(std::string result, H5T_cset_t cset, H5StringDecodeMode string_decode_mode) {
	if (!H5StringMatchesCharset(result, cset)) {
		if (H5StringDecodeModePreservesRawBytes(string_decode_mode)) {
			return Value::BLOB(const_data_ptr_cast(result.data()), result.size());
		}
		throw ErrorManager::InvalidUnicodeError(result, "value construction");
	}
	return Value(std::move(result));
}

static Value H5CreateDecodedStringValue(std::string result, const char *raw_data, size_t raw_size, H5T_cset_t cset,
                                        H5StringDecodeMode string_decode_mode) {
	if (!H5StringMatchesCharset(result, cset)) {
		if (H5StringDecodeModePreservesRawBytes(string_decode_mode)) {
			return Value::BLOB(const_data_ptr_cast(raw_data), raw_size);
		}
		throw ErrorManager::InvalidUnicodeError(result, "value construction");
	}
	return Value(std::move(result));
}

struct H5StringTypeInfo {
	bool is_variable = false;
	H5T_cset_t cset = H5T_CSET_ASCII;
	size_t fixed_length = 0;
	H5T_str_t strpad = H5T_STR_NULLTERM;
};

static H5StringTypeInfo H5InspectStringType(hid_t string_type_id, const std::string &attribute_name) {
	H5StringTypeInfo info;

	auto is_variable = H5Tis_variable_str(string_type_id);
	if (is_variable < 0) {
		throw IOException("Failed to inspect string type for attribute: " + attribute_name);
	}
	info.is_variable = is_variable > 0;

	info.cset = H5Tget_cset(string_type_id);
	if (info.cset == H5T_CSET_ERROR) {
		throw IOException("Failed to inspect string charset for attribute: " + attribute_name);
	}

	if (!info.is_variable) {
		info.fixed_length = H5Tget_size(string_type_id);
		info.strpad = H5Tget_strpad(string_type_id);
		if (info.strpad == H5T_STR_ERROR) {
			throw IOException("Failed to inspect string padding for attribute: " + attribute_name);
		}
	}
	return info;
}

static Value H5DecodeVariableStringValue(std::string result, const H5StringTypeInfo &type_info,
                                         H5StringDecodeMode string_decode_mode) {
	return H5CreateDecodedStringValue(std::move(result), type_info.cset, string_decode_mode);
}

static Value H5DecodeFixedStringValue(const char *raw_data, const H5StringTypeInfo &type_info,
                                      H5StringDecodeMode string_decode_mode) {
	auto result = H5DecodeFixedLengthString(raw_data, type_info.fixed_length, type_info.strpad);
	return H5CreateDecodedStringValue(std::move(result), raw_data, type_info.fixed_length, type_info.cset,
	                                  string_decode_mode);
}

static idx_t H5GetAttributeListLength(hid_t h5_type_id, hid_t h5_space_id, const std::string &attribute_name) {
	auto type_class = H5Tget_class(h5_type_id);
	if (type_class == H5T_ARRAY) {
		auto ndims = H5Tget_array_ndims(h5_type_id);
		if (ndims < 0) {
			throw IOException("Failed to inspect array dimensions for attribute: " + attribute_name);
		}
		if (ndims != 1) {
			throw IOException("Only 1D array attributes are supported, found " + std::to_string(ndims) + "D array");
		}
		hsize_t dims[1];
		if (H5Tget_array_dims2(h5_type_id, dims) < 0) {
			throw IOException("Failed to inspect array dimensions for attribute: " + attribute_name);
		}
		return static_cast<idx_t>(dims[0]);
	}

	auto space_class = H5Sget_simple_extent_type(h5_space_id);
	if (space_class != H5S_SIMPLE) {
		throw IOException("Failed to inspect attribute dataspace: " + attribute_name);
	}
	auto ndims = H5Sget_simple_extent_ndims(h5_space_id);
	if (ndims < 0) {
		throw IOException("Failed to inspect attribute dataspace: " + attribute_name);
	}
	if (ndims != 1) {
		throw IOException("Only 1D array attributes are supported, found " + std::to_string(ndims) + "D array");
	}
	hsize_t dims[1];
	if (H5Sget_simple_extent_dims(h5_space_id, dims, nullptr) < 0) {
		throw IOException("Failed to inspect attribute dimensions: " + attribute_name);
	}
	return static_cast<idx_t>(dims[0]);
}

static LogicalType H5StringArrayElementType(H5StringDecodeMode string_decode_mode) {
	return string_decode_mode == H5StringDecodeMode::TEXT_OR_BLOB ? LogicalType::VARIANT() : LogicalType::VARCHAR;
}

static Value H5ReadStringArrayAttributeValue(hid_t attr_id, hid_t h5_type_id, hid_t h5_space_id,
                                             const std::string &attribute_name, H5StringDecodeMode string_decode_mode) {
	auto child_type = H5StringArrayElementType(string_decode_mode);
	auto array_size = H5GetAttributeListLength(h5_type_id, h5_space_id, attribute_name);
	if (array_size == 0) {
		return Value::LIST(child_type, vector<Value>());
	}

	hid_t string_type_id = h5_type_id;
	H5TypeHandle base_type;
	auto type_class = H5Tget_class(h5_type_id);
	if (type_class == H5T_ARRAY) {
		auto base_type_id = H5Tget_super(h5_type_id);
		if (base_type_id < 0) {
			throw IOException("Failed to inspect string array attribute base type: " + attribute_name);
		}
		base_type = H5TypeHandle::TakeOwnershipOf(base_type_id);
		if (H5Tget_class(base_type.get()) != H5T_STRING) {
			throw IOException("Failed to inspect string array attribute base type: " + attribute_name);
		}
		string_type_id = base_type.get();
	} else if (type_class != H5T_STRING) {
		throw IOException("Failed to inspect string array attribute type: " + attribute_name);
	}

	auto type_info = H5InspectStringType(string_type_id, attribute_name);

	vector<Value> values;
	values.reserve(array_size);

	if (type_info.is_variable) {
		vector<char *> raw_values(array_size);
		H5ErrorSuppressor suppress;
		if (H5Aread(attr_id, h5_type_id, raw_values.data()) < 0) {
			throw IOException("Failed to read string array attribute: " + attribute_name);
		}

		auto reclaim = [&]() {
			if (H5Dvlen_reclaim(h5_type_id, h5_space_id, H5P_DEFAULT, raw_values.data()) < 0) {
				throw IOException("Failed to reclaim string array attribute: " + attribute_name);
			}
		};

		try {
			for (idx_t i = 0; i < array_size; i++) {
				if (!raw_values[i]) {
					values.emplace_back(child_type);
					continue;
				}
				values.emplace_back(
				    H5DecodeVariableStringValue(std::string(raw_values[i]), type_info, string_decode_mode));
			}
		} catch (...) {
			try {
				reclaim();
			} catch (...) {
			}
			throw;
		}
		reclaim();
		return Value::LIST(child_type, std::move(values));
	}

	vector<char> buffer(array_size * type_info.fixed_length);
	H5ErrorSuppressor suppress;
	if (H5Aread(attr_id, h5_type_id, buffer.data()) < 0) {
		throw IOException("Failed to read string array attribute: " + attribute_name);
	}

	for (idx_t i = 0; i < array_size; i++) {
		auto *raw_ptr = buffer.data() + (i * type_info.fixed_length);
		values.emplace_back(H5DecodeFixedStringValue(raw_ptr, type_info, string_decode_mode));
	}
	return Value::LIST(child_type, std::move(values));
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
		return LogicalType::LIST(element_type);
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
			duckdb_type = LogicalType::LIST(element_type);
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

Value H5ReadAttributeValue(hid_t attr_id, hid_t h5_type_id, hid_t h5_space_id, const LogicalType &resolved_type,
                           const std::string &attribute_name, H5StringDecodeMode string_decode_mode) {
	if (resolved_type.id() == LogicalTypeId::LIST) {
		auto &child_type = ListType::GetChildType(resolved_type);
		if (child_type.id() == LogicalTypeId::VARCHAR) {
			return H5ReadStringArrayAttributeValue(attr_id, h5_type_id, h5_space_id, attribute_name,
			                                       string_decode_mode);
		}
		auto array_size = H5GetAttributeListLength(h5_type_id, h5_space_id, attribute_name);
		if (array_size == 0) {
			return Value::LIST(child_type, vector<Value>());
		}
		return DispatchOnDuckDBType(child_type, [&](auto type_tag) -> Value {
			using T = typename decltype(type_tag)::type;
			std::vector<Value> values;
			values.reserve(array_size);
			std::vector<T> raw_values(array_size);
			H5ErrorSuppressor suppress;
			if (H5Aread(attr_id, h5_type_id, raw_values.data()) < 0) {
				throw IOException("Failed to read array attribute: " + attribute_name);
			}
			for (auto &value : raw_values) {
				values.push_back(CreateDuckDBValue(value));
			}
			return Value::LIST(child_type, std::move(values));
		});
	}

	if (resolved_type.id() == LogicalTypeId::VARCHAR) {
		auto type_info = H5InspectStringType(h5_type_id, attribute_name);
		if (type_info.is_variable) {
			char *str_ptr = nullptr;
			H5ErrorSuppressor suppress;
			if (H5Aread(attr_id, h5_type_id, &str_ptr) < 0) {
				throw IOException("Failed to read variable-length string attribute: " + attribute_name);
			}
			if (!str_ptr) {
				return Value(LogicalType::VARCHAR);
			}
			auto result = std::string(str_ptr);
			if (H5free_memory(str_ptr) < 0) {
				throw IOException("Failed to reclaim variable-length string attribute: " + attribute_name);
			}
			return H5DecodeVariableStringValue(std::move(result), type_info, string_decode_mode);
		}

		std::vector<char> buffer(type_info.fixed_length);
		H5ErrorSuppressor suppress;
		if (H5Aread(attr_id, h5_type_id, buffer.data()) < 0) {
			throw IOException("Failed to read fixed-length string attribute: " + attribute_name);
		}
		return H5DecodeFixedStringValue(buffer.data(), type_info, string_decode_mode);
	}

	return DispatchOnDuckDBType(resolved_type, [&](auto type_tag) -> Value {
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
