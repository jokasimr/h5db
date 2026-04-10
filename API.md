# H5DB API Reference

This document describes all functions provided by the h5db extension.

## Table Functions

`swmr := true` enables HDF5 SWMR read mode for local files. Remote URLs accept the parameter, but remote paths are
opened through the DuckDB-backed remote VFD as immutable snapshots served by `httpfs`, so `H5F_ACC_SWMR_READ` is not
used there.

### `h5_tree(filename, projected_attributes...)`

Lists all groups and datasets in an HDF5 file. Selected HDF5 attributes can be
projected as additional columns with `h5_attr(...)`.

**Parameters:**
- `filename` (VARCHAR): Local path or remote URL to the HDF5 file. Remote schemes are handled through DuckDB `httpfs`
  (for example `http://`, `https://`, `s3://`, `s3a://`, `s3n://`, `r2://`, `gcs://`, `gs://`, `hf://`).
- `projected_attributes` (variadic, optional): Zero or more projected attribute markers:
  - `h5_attr(name, default_value)`
  - `h5_alias(alias_name, h5_attr(name, default_value))`
- `swmr` (BOOLEAN, named, optional): Open in SWMR read mode (default: `false`)

**Returns:** Table with columns:
- `path` (VARCHAR): Object name/path
- `type` (VARCHAR): Object type (`group` or `dataset`)
- `dtype` (VARCHAR): Data type for datasets (e.g., `int32`, `float64`, `string`)
- `shape` (LIST<UBIGINT>): Dataset dimensions
  - `NULL` for groups
  - `[]` for scalar datasets
  - `[d0, d1, ...]` for array datasets
- one additional column per projected attribute
  - column name = resolved attribute name, unless overridden by `h5_alias(...)`
  - column type = type of `default_value`

**Projected Attribute Semantics:**
- `name` must resolve to a non-`NULL` `VARCHAR` value at bind time
- constant expressions such as `lower('STRING_ATTR')` are allowed
- row-dependent expressions are rejected by DuckDB as unsupported lateral parameters for `h5_tree`
- `default_value` must be a bind-time constant expression with a concrete type
- typed `NULL` defaults are allowed, e.g. `NULL::VARCHAR`
- if an object has the projected attribute, `h5_tree` reads it using the same conversion rules as `h5_attributes()`
  and casts it to the declared output type
- if an object does not have the projected attribute, `h5_tree` emits `default_value`
- projected output names must be unique; duplicate projected names or collisions with `path`, `type`, `dtype`, or
  `shape` fail at bind time with DuckDB's duplicate-column error

**Projected Attribute Limitations:**
- scalar numeric attributes and scalar string attributes are supported
- simple 1D numeric arrays are supported
- multidimensional attribute dataspaces are not supported
- string array attributes are not supported
- other unsupported HDF5 attribute forms follow the same behavior as `h5_attributes()`

**Example:**
```sql
SELECT * FROM h5_tree('data.h5');
SELECT * FROM h5_tree('data.h5', swmr := true);

-- Project an attribute as an extra column
SELECT path, type, NX_class
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class', NULL::VARCHAR)
);

-- Rename a projected attribute column
SELECT path, type, time
FROM h5_tree(
    'data.h5',
    h5_alias('time', h5_attr('count_time', 0::DOUBLE))
);
```

---

### `h5_read(filename, dataset_path, ...)`

Reads data from one or more datasets in an HDF5 file.

**Parameters:**
- `filename` (VARCHAR): Local path or remote URL to the HDF5 file. Remote schemes are handled through DuckDB `httpfs`
  (for example `http://`, `https://`, `s3://`, `s3a://`, `s3n://`, `r2://`, `gcs://`, `gs://`, `hf://`).
- `dataset_path` (VARCHAR or STRUCT): Dataset path(s) to read. Use `h5_rse()` for run-start encoded columns
- `h5_index()` can be provided to add a virtual index column named `index`
- `h5_alias(name, definition)` can be used to rename a column definition
- Additional dataset paths can be provided (variadic arguments)
- `swmr` (BOOLEAN, named, optional): Open in SWMR read mode (default: `false`)

**Returns:** Table with one column per dataset

**Column Naming:** Column names are extracted from the last component of the dataset path (e.g., `/group/data` → column `data`)

**Scalar Datasets:**
- Scalar (rank-0) datasets are returned as constant columns.
- If all selected datasets are scalar, `h5_read()` returns a single row.
- If any non-scalar dataset is present, scalar columns are broadcast to the row count of the non-scalar datasets.
- If multiple non-scalar regular datasets are selected, the output row count is the minimum outer dimension among them.

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

-- Add a virtual index column
SELECT * FROM h5_read('data.h5', h5_index(), '/measurements');

-- Rename a column definition
SELECT * FROM h5_read('data.h5', h5_alias('idx', h5_index()), '/measurements');

-- Enable SWMR read mode
SELECT * FROM h5_read('data.h5', '/measurements', swmr := true);
```

---

### `h5_attributes(filename, object_path)`

Reads attributes from a dataset or group.

**Parameters:**
- `filename` (VARCHAR): Local path or remote URL to the HDF5 file. Remote schemes are handled through DuckDB `httpfs`
  (for example `http://`, `https://`, `s3://`, `s3a://`, `s3n://`, `r2://`, `gcs://`, `gs://`, `hf://`).
- `object_path` (VARCHAR): Path to the dataset or group (use empty string or '/' for root)
- `swmr` (BOOLEAN, named, optional): Open in SWMR read mode (default: `false`)

**Returns:** Single-row table where each column represents one attribute
- Column names are the attribute names
- Column types match the attribute types (numeric, string, or arrays)
- If the target object has no attributes, the function raises `IO Error: Object has no attributes: ...`

**Type Support:**
- Numeric scalars, string scalars, and 1D numeric arrays
- HDF5 `H5T_ARRAY`-typed attributes are supported only when they are 1D and numeric
- String array attributes are not currently supported

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

-- Enable SWMR read mode
SELECT * FROM h5_attributes('data.h5', '/measurements', swmr := true);
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
- `run_starts` must be an integer dataset, strictly increasing
- `run_starts` and `values` must have the same length
- At least one regular (non-RSE) column must be present in the `h5_read()` call to determine total row count

**Notes:**
- If `run_starts[0] > 0`, rows before the first run start are returned as NULLs.

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

### `h5_index()`

Adds a virtual index column for the outermost dimension when used with `h5_read()`.

**Parameters:** none

**Returns:** STRUCT tag used by `h5_read()`

**Default column name:** `index`

**Example:**
```sql
SELECT index, measurements FROM h5_read('data.h5', h5_index(), '/measurements');
```

**Predicate pushdown:** range filters on the index column (e.g., `index >= 100`, `index BETWEEN 10 AND 20`) are used to
reduce I/O when the bounds are static constants.

---

### `h5_attr(name, default_value)`

Creates a projected-attribute definition for use with `h5_tree()`.

**Parameters:**
- `name` (VARCHAR): Attribute name to read. Any non-`NULL` bind-time-resolved `VARCHAR` expression is allowed.
- `default_value` (ANY): Bind-time constant default value. Its type becomes the output type of the projected column.

**Returns:** STRUCT wrapper used by `h5_tree()`

**Notes:**
- typed `NULL` defaults such as `NULL::VARCHAR` are allowed
- untyped `NULL` defaults are rejected
- missing attributes use `default_value`
- present attributes are converted like `h5_attributes()` and then cast to the type of `default_value`

**Example:**
```sql
SELECT path, type, NX_class
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class', NULL::VARCHAR)
);
```

---

### `h5_alias(name, definition)`

Renames a column definition when used with `h5_read()` or a projected attribute
definition when used with `h5_tree()`.

**Parameters:**
- `name` (VARCHAR): Column name to use in the output
- `definition` (VARCHAR or STRUCT): A dataset path, a column definition like `h5_rse()` or `h5_index()`, or a
  projected attribute definition from `h5_attr()`

**Returns:** STRUCT wrapper used by `h5_read()` and `h5_tree()`

**Example:**
```sql
SELECT * FROM h5_read(
    'data.h5',
    h5_alias('idx', h5_index()),
    h5_alias('temp_c', '/temperature')
);

SELECT path, time
FROM h5_tree(
    'data.h5',
    h5_alias('time', h5_attr('count_time', 0::DOUBLE))
);
```

---

## Test Functions

These functions are provided for testing and verification purposes:

### `h5db_version(name)`
Returns the HDF5 library version being used.

---

## Settings

### `h5db_swmr_default` (BOOLEAN)
Default SWMR read mode for h5db table functions. Defaults to `false`.

**Example:**
```sql
SET h5db_swmr_default = true;
```

### `h5db_batch_size` (VARCHAR)
Target batch size used by `h5_read` for numeric chunk caching and scan chunk sizing. Accepts DuckDB memory-size
strings such as `'1MB'`, `'8MB'`, `'512KB'`. Defaults to `'1MB'`. Values above `1GB` are clamped to `1GB`.

This is a target, not a hard memory cap. For HDF5 chunked datasets, `h5_read` may align cache chunks upward to the
dataset's first-dimension HDF5 chunk size.

**Example:**
```sql
SET h5db_batch_size = '4MB';
```

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
Note: Multi-dimensional string datasets are not currently supported.

---

## Error Handling

All functions provide clear error messages for common issues:

- **File not found**: "IO Error: Failed to open HDF5 file"
- **Invalid path**: "IO Error: Dataset or group not found"
- **Unsupported type**: "IO Error: Unsupported HDF5 type"
- **Invalid RSE data**: "IO Error: RSE run_starts must be strictly increasing"
- **No attributes**: "IO Error: Object has no attributes"

---

## Performance Notes

- **Projection pushdown**: Only reads columns actually used by your query, skipping unused datasets entirely for significant performance gains
- **Predicate pushdown**: Range-like filters with static constants are applied during scan for RSE and `h5_index()` columns
  to reduce I/O (supported: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`; not supported: `!=`, `IS DISTINCT FROM`, or expressions
  like `index + 1 > 10`)
- **Chunked reading**: Data is read in chunks with optimized cache management for memory efficiency
- **Hyperslab selection**: Uses HDF5's hyperslab selection for efficient partial reads
- **RSE optimization**: Run-start encoded data is expanded on-the-fly with O(1) amortized cost per row
- **Parallel scanning**: Multiple threads can read different row ranges simultaneously for improved throughput

---

## Limitations

1. **Compound types** (HDF5 structs) are not currently supported
2. **Enum types** are not currently supported
3. **Opaque, bitfield, reference, time-like, and non-string variable-length HDF5 type classes** are not supported
4. **Datasets with >4 dimensions** are not supported
5. **Multi-dimensional string datasets** are not supported
6. **Attribute string arrays** are not supported
7. **Attribute multidimensional dataspaces** are not supported (only scalar and 1D)

---

## See Also

- [README.md](README.md) - Project overview and quick start
- [RSE_USAGE.md](RSE_USAGE.md) - Detailed guide to run-start encoding
- [docs/DEVELOPER.md](docs/DEVELOPER.md) - Development and building guide
