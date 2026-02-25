#pragma once

#include "duckdb.hpp"
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace duckdb {

// Global mutex for all HDF5 operations
// HDF5 is not guaranteed to be thread-safe, so we serialize all HDF5 calls
// This prevents crashes when DuckDB parallelizes table function execution
extern std::recursive_mutex hdf5_global_mutex;

// Resolve SWMR read mode from named parameters or default setting.
// Named parameter "swmr" takes precedence over h5db_swmr_default.
bool ResolveSwmrOption(ClientContext &context, const named_parameter_map_t &named_parameters);

// Type tag for compile-time type dispatch
template <typename T>
struct TypeTag {
	using type = T;
};

// Type dispatcher - centralizes all type switching logic
// Takes a DuckDB LogicalType and a lambda/function that accepts TypeTag<T>
template <typename Func>
auto DispatchOnDuckDBType(LogicalType logical_type, Func &&func) {
	switch (logical_type.id()) {
	case LogicalTypeId::TINYINT:
		return func(TypeTag<int8_t> {});
	case LogicalTypeId::SMALLINT:
		return func(TypeTag<int16_t> {});
	case LogicalTypeId::INTEGER:
		return func(TypeTag<int32_t> {});
	case LogicalTypeId::BIGINT:
		return func(TypeTag<int64_t> {});
	case LogicalTypeId::UTINYINT:
		return func(TypeTag<uint8_t> {});
	case LogicalTypeId::USMALLINT:
		return func(TypeTag<uint16_t> {});
	case LogicalTypeId::UINTEGER:
		return func(TypeTag<uint32_t> {});
	case LogicalTypeId::UBIGINT:
		return func(TypeTag<uint64_t> {});
	case LogicalTypeId::FLOAT:
		return func(TypeTag<float> {});
	case LogicalTypeId::DOUBLE:
		return func(TypeTag<double> {});
	case LogicalTypeId::VARCHAR:
		return func(TypeTag<string> {});
	default:
		throw IOException("Unsupported DuckDB type");
	}
}

// Numeric-only type dispatcher (excludes VARCHAR)
template <typename Func>
auto DispatchOnNumericType(LogicalType logical_type, Func &&func) {
	switch (logical_type.id()) {
	case LogicalTypeId::TINYINT:
		return func(TypeTag<int8_t> {});
	case LogicalTypeId::SMALLINT:
		return func(TypeTag<int16_t> {});
	case LogicalTypeId::INTEGER:
		return func(TypeTag<int32_t> {});
	case LogicalTypeId::BIGINT:
		return func(TypeTag<int64_t> {});
	case LogicalTypeId::UTINYINT:
		return func(TypeTag<uint8_t> {});
	case LogicalTypeId::USMALLINT:
		return func(TypeTag<uint16_t> {});
	case LogicalTypeId::UINTEGER:
		return func(TypeTag<uint32_t> {});
	case LogicalTypeId::UBIGINT:
		return func(TypeTag<uint64_t> {});
	case LogicalTypeId::FLOAT:
		return func(TypeTag<float> {});
	case LogicalTypeId::DOUBLE:
		return func(TypeTag<double> {});
	default:
		throw IOException("Unsupported DuckDB type");
	}
}

// Map C++ numeric type to native HDF5 memory type
template <typename T>
inline hid_t GetNativeH5Type() {
	static_assert(!std::is_same_v<T, string>, "No native HDF5 type for string");
	if constexpr (std::is_same_v<T, int8_t>) {
		return H5T_NATIVE_INT8;
	} else if constexpr (std::is_same_v<T, int16_t>) {
		return H5T_NATIVE_INT16;
	} else if constexpr (std::is_same_v<T, int32_t>) {
		return H5T_NATIVE_INT32;
	} else if constexpr (std::is_same_v<T, int64_t>) {
		return H5T_NATIVE_INT64;
	} else if constexpr (std::is_same_v<T, uint8_t>) {
		return H5T_NATIVE_UINT8;
	} else if constexpr (std::is_same_v<T, uint16_t>) {
		return H5T_NATIVE_UINT16;
	} else if constexpr (std::is_same_v<T, uint32_t>) {
		return H5T_NATIVE_UINT32;
	} else if constexpr (std::is_same_v<T, uint64_t>) {
		return H5T_NATIVE_UINT64;
	} else if constexpr (std::is_same_v<T, float>) {
		return H5T_NATIVE_FLOAT;
	} else if constexpr (std::is_same_v<T, double>) {
		return H5T_NATIVE_DOUBLE;
	}
	throw IOException("Unsupported DuckDB type");
}

} // namespace duckdb
