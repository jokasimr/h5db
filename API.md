# H5DB API Reference

This document describes all functions provided by the h5db extension.

## Table Functions

### `h5_tree(filename)`

Lists all groups and datasets in an HDF5 file.

**Parameters:**
- `filename` (VARCHAR): Path to the HDF5 file

**Returns:** Table with columns:
- `name` (VARCHAR): Object name/path
- `type` (VARCHAR): Object type (e.g., "int32", "float64", "string", "GROUP")
- `ndims` (INTEGER): Number of dimensions (0 for scalars, 1+ for arrays)
- `shape` (VARCHAR): Shape of the dataset (e.g., "[100, 4, 3]")

**Example:**
```sql
SELECT * FROM h5_tree('data.h5');
```

---

### `h5_read(filename, dataset_path, ...)`

Reads data from one or more datasets in an HDF5 file.

**Parameters:**
- `filename` (VARCHAR): Path to the HDF5 file
- `dataset_path` (VARCHAR or RSE STRUCT): Dataset path(s) to read. Use `h5_rse()` for run-start encoded columns
- Additional dataset paths can be provided (variadic arguments)

**Returns:** Table with one column per dataset

**Column Naming:** Column names are extracted from the last component of the dataset path (e.g., `/group/data` → column `data`)

**Type Support:**
- Numeric: int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64
- Strings: fixed-length and variable-length
- Arrays: 1D-4D multi-dimensional arrays

**Examples:**
```sql
-- Read a single dataset
SELECT * FROM h5_read('data.h5', '/measurements');

-- Read multiple datasets
SELECT * FROM h5_read('data.h5', '/timestamps', '/temperatures', '/pressures');

-- Read from nested groups
SELECT * FROM h5_read('data.h5', '/experiment1/group_a/data');

-- Mix regular and run-start encoded columns
SELECT * FROM h5_read(
    'data.h5',
    '/timestamp',
    h5_rse('/status_run_starts', '/status_values')
);
```

---

### `h5_attributes(filename, object_path)`

Reads attributes from a dataset or group.

**Parameters:**
- `filename` (VARCHAR): Path to the HDF5 file
- `object_path` (VARCHAR): Path to the dataset or group (use empty string or '/' for root)

**Returns:** Single-row table where each column represents one attribute
- Column names are the attribute names
- Column types match the attribute types (numeric, string, or arrays)

**Type Support:**
- Same as `h5_read()`: all numeric types, strings, and 1D arrays

**Examples:**
```sql
-- Read attributes from a dataset
SELECT * FROM h5_attributes('data.h5', '/measurements');

-- Read specific attributes
SELECT units, description FROM h5_attributes('data.h5', '/temperature');

-- Read attributes from a group
SELECT * FROM h5_attributes('data.h5', '/experiment1');

-- Read file-level (root) attributes
SELECT * FROM h5_attributes('data.h5', '/');
```

---

## Scalar Functions

### `h5_rse(run_starts_path, values_path)`

Creates a run-start encoded (RSE) column specification for use with `h5_read()`.

**Parameters:**
- `run_starts_path` (VARCHAR): Path to dataset containing run start indices
- `values_path` (VARCHAR): Path to dataset containing values for each run

**Returns:** STRUCT with fields `{encoding, run_starts, values}`

**Requirements:**
- `run_starts` must be an integer dataset starting at 0 and strictly increasing
- `run_starts` and `values` must have the same length
- At least one regular (non-RSE) column must be present in the `h5_read()` call to determine total row count

**Example:**
```sql
SELECT * FROM h5_read(
    'data.h5',
    '/row_index',                                  -- Regular column determines row count
    h5_rse('/state_starts', '/state_vals')        -- RSE column expands based on run_starts
);
```

For detailed information on run-start encoding, see [RSE_USAGE.md](RSE_USAGE.md).

---

## Test Functions

These functions are provided for testing and verification purposes:

### `h5db(name)`
Returns a test string. Used for verifying basic extension loading.

### `h5db_version(name)`
Returns the HDF5 library version being used.

### `h5db_openssl_version(name)`
Returns the OpenSSL version (if linked).

---

## Type Mapping

### HDF5 → DuckDB Type Conversion

| HDF5 Type | Size | DuckDB Type |
|-----------|------|-------------|
| H5T_INTEGER (signed) | 1 byte | TINYINT |
| H5T_INTEGER (signed) | 2 bytes | SMALLINT |
| H5T_INTEGER (signed) | 4 bytes | INTEGER |
| H5T_INTEGER (signed) | 8 bytes | BIGINT |
| H5T_INTEGER (unsigned) | 1 byte | UTINYINT |
| H5T_INTEGER (unsigned) | 2 bytes | USMALLINT |
| H5T_INTEGER (unsigned) | 4 bytes | UINTEGER |
| H5T_INTEGER (unsigned) | 8 bytes | UBIGINT |
| H5T_FLOAT | 4 bytes | FLOAT |
| H5T_FLOAT | 8 bytes | DOUBLE |
| H5T_STRING | variable/fixed | VARCHAR |

### Multi-Dimensional Arrays

HDF5 arrays are mapped to DuckDB ARRAY types:

| HDF5 Shape | DuckDB Type |
|------------|-------------|
| [N] | N rows of scalar |
| [N, M] | N rows of TYPE[M] |
| [N, M, P] | N rows of TYPE[P][M] |
| [N, M, P, Q] | N rows of TYPE[Q][P][M] |

Note: Arrays with more than 4 dimensions are not currently supported.

---

## Error Handling

All functions provide clear error messages for common issues:

- **File not found**: "IO Error: Failed to open HDF5 file"
- **Invalid path**: "IO Error: Dataset or group not found"
- **Unsupported type**: "IO Error: Unsupported HDF5 type"
- **Invalid RSE data**: "IO Error: run_starts must begin at 0 and be strictly increasing"

---

## Performance Notes

- **Projection pushdown**: Only reads columns actually used by your query, skipping unused datasets entirely for significant performance gains
- **Predicate pushdown**: For RSE columns, filters are applied during scan to reduce I/O by computing valid row ranges
- **Chunked reading**: Data is read in chunks with optimized cache management for memory efficiency
- **Hyperslab selection**: Uses HDF5's hyperslab selection for efficient partial reads
- **RSE optimization**: Run-start encoded data is expanded on-the-fly with O(1) amortized cost per row
- **Parallel scanning**: Multiple threads can read different row ranges simultaneously for improved throughput

---

## Limitations

1. **Compound types** (HDF5 structs) are not currently supported
2. **Enum types** are not currently supported
3. **Datasets with >4 dimensions** are not supported
4. **Reference types** (links to other datasets) are not supported
5. **Variable-length array types** are not supported (fixed-size arrays only)

---

## See Also

- [README.md](README.md) - Project overview and quick start
- [RSE_USAGE.md](RSE_USAGE.md) - Detailed guide to run-start encoding
- [docs/DEVELOPER.md](docs/DEVELOPER.md) - Development and building guide
