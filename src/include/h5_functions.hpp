#pragma once

#include "duckdb.hpp"
#include "h5_raii.hpp"
#include <hdf5.h>
#include <vector>

namespace duckdb {

enum class H5StringDecodeMode : uint8_t { STRICT_TEXT, TEXT_OR_BLOB };

struct H5OpenedAttribute {
	H5AttributeHandle attr;
	H5TypeHandle type;
	H5DataspaceHandle space;
};

// Validate whether a decoded HDF5 string satisfies its declared character set.
bool H5StringMatchesCharset(const std::string &value, H5T_cset_t cset);

// Decode a fixed-length HDF5 string according to its declared padding mode.
std::string H5DecodeFixedLengthString(const char *raw_data, size_t raw_size, H5T_str_t strpad);

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

// Resolve the DuckDB LogicalType for an HDF5 attribute when unsupported attribute forms should be skipped.
// Returns false for unsupported attribute types/dataspaces and throws on genuine HDF5 inspection failures.
bool H5TryResolveAttributeLogicalType(hid_t type_id, hid_t space_id, LogicalType &duckdb_type);

// Read an HDF5 attribute into a DuckDB Value using a previously resolved DuckDB source type.
// When TEXT_OR_BLOB is requested for string values, the returned Value may preserve invalid bytes as BLOB/VARIANT
// instead of matching the nominal resolved type exactly.
Value H5ReadAttributeValue(hid_t attr_id, hid_t h5_type_id, hid_t h5_space_id, const LogicalType &resolved_type,
                           const std::string &attribute_name,
                           H5StringDecodeMode string_decode_mode = H5StringDecodeMode::STRICT_TEXT);

// Open an attribute and inspect its HDF5 type and dataspace.
H5OpenedAttribute H5OpenAttribute(hid_t object_id, const std::string &attribute_name);

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
