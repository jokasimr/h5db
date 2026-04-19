#pragma once

#include "duckdb.hpp"
#include <hdf5.h>
#include <vector>

namespace duckdb {

// Helper function to get HDF5 type as string
std::string H5TypeToString(hid_t type_id);

// Helper function to get dataset dimensions as a list
std::vector<hsize_t> H5GetShape(hid_t dataset_id);

// Helper function to map HDF5 type to DuckDB LogicalType
LogicalType H5TypeToDuckDBType(hid_t type_id);

// Helper function to map HDF5 attribute type (including arrays) to DuckDB LogicalType
LogicalType H5AttributeTypeToDuckDBType(hid_t type_id);

// Resolve the DuckDB LogicalType for an HDF5 attribute from its type and dataspace.
LogicalType H5ResolveAttributeLogicalType(hid_t type_id, hid_t space_id, const std::string &attribute_name);

// Read an HDF5 attribute into a DuckDB Value using a previously resolved DuckDB type.
Value H5ReadAttributeValue(hid_t attr_id, hid_t h5_type_id, const LogicalType &duckdb_type,
                           const std::string &attribute_name);

// Table function for listing HDF5 file contents
void RegisterH5TreeFunction(ExtensionLoader &loader);

// Scalar and table functions for listing immediate HDF5 group contents
void RegisterH5LsFunctions(ExtensionLoader &loader);

// Table function for reading HDF5 datasets
void RegisterH5ReadFunction(ExtensionLoader &loader);

// Scalar function for creating RSE (run-start encoded) column specs
void RegisterH5RseFunction(ExtensionLoader &loader);

// Scalar function for aliasing column specs with custom names
void RegisterH5AliasFunction(ExtensionLoader &loader);

// Scalar function for projecting HDF5 attributes in h5_tree
void RegisterH5AttrFunction(ExtensionLoader &loader);

// Scalar function for adding a virtual index column
void RegisterH5IndexFunction(ExtensionLoader &loader);

// Table function for reading HDF5 attributes
void RegisterH5AttributesFunction(ExtensionLoader &loader);

// Secret type/provider registration for native sftp support
void RegisterH5SftpSecrets(ExtensionLoader &loader);

} // namespace duckdb
