#pragma once

#include "duckdb.hpp"
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace duckdb {

// Global mutex for all HDF5 operations
// HDF5 is not guaranteed to be thread-safe, so we serialize all HDF5 calls
// This prevents crashes when DuckDB parallelizes table function execution
extern std::mutex hdf5_global_mutex;

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

} // namespace duckdb
