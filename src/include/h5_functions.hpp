#pragma once

#include "duckdb.hpp"
#include <hdf5.h>

namespace duckdb {

// Helper function to get HDF5 type as string
std::string H5TypeToString(hid_t type_id);

// Helper function to get dataset dimensions as string
std::string H5GetShapeString(hid_t dataset_id);

// Helper function to map HDF5 type to DuckDB LogicalType
LogicalType H5TypeToDuckDBType(hid_t type_id);

// Helper function to map HDF5 attribute type (including arrays) to DuckDB LogicalType
LogicalType H5AttributeTypeToDuckDBType(hid_t type_id);

// Table function for listing HDF5 file contents
void RegisterH5TreeFunction(ExtensionLoader &loader);

// Table function for reading HDF5 datasets
void RegisterH5ReadFunction(ExtensionLoader &loader);

// Scalar function for creating RSE (run-start encoded) column specs
void RegisterH5RseFunction(ExtensionLoader &loader);

// Scalar function for aliasing column specs with custom names
void RegisterH5AliasFunction(ExtensionLoader &loader);

// Table function for reading HDF5 attributes
void RegisterH5AttributesFunction(ExtensionLoader &loader);

} // namespace duckdb
