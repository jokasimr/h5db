# H5DB API Reference

This document describes the public SQL functions provided by the h5db extension.

## Remote Access

The table-valued h5db functions accept `swmr := true` for local files. Remote table-function opens accept the
parameter, but remote paths are opened as immutable snapshots, so `H5F_ACC_SWMR_READ` is not used there.

All file-opening h5db functions accept local paths or remote URLs as `filename`.

- DuckDB-backed remote schemes such as `http://`, `https://`, `s3://`, `s3a://`, `s3n://`, `r2://`, `gcs://`,
  `gs://`, and `hf://` are opened through DuckDB's filesystem stack. h5db auto-loads the required DuckDB extension
  when needed.
- `sftp://` URLs are handled by h5db's built-in SFTP backend.
- Remote opens are immutable snapshot reads. This applies to both DuckDB-backed remote paths and SFTP.

### Filename Patterns and Filename Lists

The table-valued `h5_tree(...)`, table-valued `h5_ls(...)`, `h5_read(...)`, and `h5_attributes(...)`
accept:

- a single filename or URL (`VARCHAR`)
- a glob pattern (`VARCHAR`)
- a list of exact filenames/URLs and/or glob patterns (`VARCHAR[]`)

Globbing is supported for local paths, for `sftp://` URLs, and for DuckDB-backed
remote schemes when the underlying DuckDB filesystem supports globbing.

**Glob syntax:**
- `*`
- `?`
- bracket classes such as `[12]` or `[a-z]`
- one recursive `**` segment per pattern

**Multi-file semantics:**
- A single glob expands to lexicographically sorted matches.
- A `VARCHAR[]` input expands entries left-to-right.
- Duplicate files are preserved and are processed more than once.
- A pattern that matches no files raises an error.
- `h5_read(...)` concatenates rows file by file.
- `h5_attributes(...)` emits one row per matched file.
- `h5_index()` is the outermost-dimension row index within each matched file.
- Table-valued `h5_tree(...)`, table-valued `h5_ls(...)`, `h5_read(...)`, and `h5_attributes(...)` expose a hidden
  virtual `filename` column that can be referenced explicitly.
- `filename := true` adds `filename` to the visible output schema.
- `filename := 'source_file'` adds the same visible filename column but uses the provided column name instead of `filename`.
  In that case the column must be referenced as `source_file`; hidden `filename` is not also available.
- All matched files in `h5_read(...)` must have compatible column definitions.
- All matched files in `h5_attributes(...)` must expose the same attributes in the same order with the same names and
  types.
- Scalar `h5_ls(...)` accepts one filename/URL expression per row and does not expand filename lists or glob patterns.
- For local paths and DuckDB-backed remote schemes, glob expansion uses
  DuckDB's filesystem stack. For `sftp://` URLs, glob expansion is handled by
  h5db's SFTP backend.
- In both cases, glob expansion follows DuckDB's other multi-file reader
  semantics such as `read_parquet(...)`. In particular, recursive `**` does not
  traverse symlink directories.
- On Windows, HDF5 1.14.6 with its native file driver fails to open a file
  symlink when the symlink itself is passed as the filename. h5db does not
  resolve those paths before calling HDF5, so final-component file symlink
  paths are not supported there.

**Filename filter pruning:**

`h5_tree(...)` and table-valued `h5_ls(...)` can apply selective filters on the virtual `filename` column before
opening each expanded file. This includes hidden `filename` filters and filters on the visible renamed column when
using `filename := 'source_file'`.

Useful shapes include simple comparisons, `IN`, `LIKE`, and filename predicates inside `AND` filters, as long as DuckDB
pushes the filter into the table function and the predicate can be evaluated from the filename alone. Dynamic filters
whose values come from joins, semi-joins, scalar subqueries, or later query blocks may still open all expanded files.

This file-open pruning currently applies only to `h5_tree(...)` and table-valued `h5_ls(...)`. `h5_read(...)` and
`h5_attributes(...)` do not use `filename` filters to avoid opening files.

**Examples:**

```sql
SELECT * FROM h5_tree('runs/**/*.h5');

SELECT * FROM h5_read(
    ['runs/run_001.h5', 'runs/run_*.h5'],
    '/counts'
);

SELECT * FROM h5_read(
    'sftp://beamline.example.org/data/run_*.h5',
    '/entry/data'
);

SELECT filename, units
FROM h5_attributes('runs/run_*.h5', '/entry/data');

SELECT path, type
FROM h5_tree('runs/**/*.h5')
WHERE filename LIKE '%/run_042.h5';

SELECT source_file, path
FROM h5_ls('runs/run_*.h5', '/entry', filename := 'source_file')
WHERE source_file IN ('runs/run_001.h5', 'runs/run_002.h5');
```

### SFTP Secrets

Before using `sftp://` URLs, create a DuckDB secret of type `sftp` whose scope matches the URL you want to access.

**Example:**

```sql
CREATE OR REPLACE SECRET beamline_sftp (
    TYPE sftp,
    SCOPE 'sftp://beamline.example.org/',
    USERNAME 'alice',
    USE_AGENT true,
    KNOWN_HOSTS_PATH '/home/alice/.ssh/known_hosts'
);

SELECT * FROM h5_read(
    'sftp://beamline.example.org/data/run001.h5',
    '/entry/data'
);
```

**Required secret fields:**
- `USERNAME`
- exactly one of `PASSWORD`, `KEY_PATH`, or `USE_AGENT`
- at least one of `KNOWN_HOSTS_PATH` or `HOST_KEY_FINGERPRINT`

**Optional secret fields:**
- `KEY_PASSPHRASE`: passphrase for an encrypted private key
- `PORT`: default `22`
- `HOST_KEY_ALGORITHMS`: libssh2 host-key preference string, for example
  `'ssh-ed25519,ecdsa-sha2-nistp256'`

**Notes:**
- `HOST_KEY_FINGERPRINT` is the lowercase hex SHA1 host-key fingerprint.
- Password auth via `PASSWORD` and explicit key-file auth via `KEY_PATH` with
  optional `KEY_PASSPHRASE` are also supported.
- `USE_AGENT` uses libssh2's SSH agent support. On Unix-like systems this uses
  the agent exposed through `SSH_AUTH_SOCK`. On Windows, libssh2 uses its
  supported agent backends (for example Pageant/OpenSSH) when available.
- If a username or port is embedded in the `sftp://` URL, it must match the selected secret.

## Table Functions

### `h5_tree(filename_or_filenames, projected_attributes...)`

Recursively lists namespace entries in an HDF5 file. The result is path-oriented:
each absolute namespace path is emitted as its own row, even when multiple paths
resolve to the same underlying object. Selected HDF5 attributes can be projected
as additional columns with `h5_attr(...)`.

**Parameters:**
- `filename_or_filenames` (VARCHAR or VARCHAR[]): Local path or remote URL to the HDF5 file, a glob
  pattern, or a list of exact filenames/URLs and/or glob patterns. See [Remote Access](#remote-access).
- `projected_attributes` (variadic, optional): Zero or more projected attribute markers:
  - `h5_attr()`
  - `h5_attr(name)`
  - `h5_attr(name, default_value)`
  - `h5_alias(alias_name, h5_attr(...))`
- `filename` (BOOLEAN or VARCHAR, named, optional): Add a visible filename column. `true` uses the column name
  `filename`; a string uses the provided column name instead. Without this option, hidden virtual `filename`
  remains available by explicit reference.
- `swmr` (BOOLEAN, named, optional): Open in SWMR read mode (default: `false`)

**Returns:** Table with columns:
- `path` (VARCHAR): Absolute namespace path for the emitted row
- `type` (VARCHAR): Entry type:
  - `group`
  - `dataset`
  - `datatype`
  - `link` for dangling or otherwise unresolved local links
  - `external` for external links
- `dtype` (VARCHAR): Data type for datasets (e.g., `int32`, `float64`, `string`)
- `shape` (UBIGINT[]): Dataset dimensions
  - `NULL` for non-dataset rows
  - `[]` for scalar datasets
  - `[d0, d1, ...]` for array datasets
- one additional column per projected attribute
  - `h5_attr()` produces column `h5_attr` with type `MAP(VARCHAR, VARIANT)`
  - `h5_attr(name)` produces column `name` with type `VARIANT`
  - `h5_attr(name, default_value)` produces column `name` with the type of `default_value`
  - `h5_alias(...)` overrides the default output name
- hidden virtual column `filename` (VARCHAR), available by explicit reference

**Traversal Semantics:**
- `/` is returned as a `group` row.
- Hard-link aliases, soft-link aliases, and other namespace aliases are path-complete:
  if two paths resolve to the same local object, both paths appear.
- Resolved group aliases are traversed recursively in a cycle-safe way.
- Dangling links are returned as `type = 'link'` leaf rows.
- External links are returned as `type = 'external'` leaf rows and are not traversed.
- Projected attributes and dataset metadata are populated only for resolved local
  rows. Unresolved and external rows use projected defaults.

**Projected Attributes:**
- Projected attributes are declared with `h5_attr(...)` and may be renamed with `h5_alias(...)`.
- Only resolved local rows read attribute values. Unresolved and external rows use projected defaults.
- Projected output names must be unique under DuckDB's case-insensitive identifier matching; duplicate projected names
  or collisions with `path`, `type`, `dtype`, or `shape` fail at bind time.
- See `h5_attr(...)` below for bind-time rules, default handling, string behavior, and supported attribute forms.

**Example:**
```sql
SELECT * FROM h5_tree('data.h5');
SELECT * FROM h5_tree('data.h5', swmr := true);
SELECT * FROM h5_tree('runs/run_*.h5');
SELECT * FROM h5_tree(['runs/calibration.h5', 'runs/run_*.h5']);

-- Project an attribute as an extra column
SELECT path, type, NX_class
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class', NULL::VARCHAR)
);

-- Distinguish identical paths across multiple files
SELECT filename, path
FROM h5_tree('runs/run_*.h5')
WHERE path = '/entry/data';

-- Project all attributes as one map-valued column
SELECT path, h5_attr
FROM h5_tree(
    'data.h5',
    h5_attr()
);

-- Rename a projected attribute column
SELECT path, type, time
FROM h5_tree(
    'data.h5',
    h5_alias('time', h5_attr('count_time', 0::DOUBLE))
);
```

---

### `h5_ls(filename_or_filenames[, group_path], projected_attributes...)`

Lists the immediate children of a group. This is the shallow counterpart to
`h5_tree()`. The scalar form is documented below.

The table form:

```sql
SELECT * FROM h5_ls('data.h5');
SELECT * FROM h5_ls('data.h5', '/entry/instrument');
```

returns the same row shape as `h5_tree()`, but only for the immediate children of
the requested group.

**Parameters:**
- `filename_or_filenames` (VARCHAR or VARCHAR[]): Local path or remote URL to the HDF5 file, a glob
  pattern, or a list of exact filenames/URLs and/or glob patterns. See [Remote Access](#remote-access).
- `group_path` (VARCHAR, optional): Group path to list. Defaults to `/` in the table form.
- `projected_attributes` (variadic, optional): Zero or more projected attribute markers:
  - `h5_attr()`
  - `h5_attr(name)`
  - `h5_attr(name, default_value)`
  - `h5_alias(alias_name, h5_attr(...))`
- `filename` (BOOLEAN or VARCHAR, named, optional): Add a visible filename column. `true` uses the column name
  `filename`; a string uses the provided column name instead. Without this option, hidden virtual `filename`
  remains available by explicit reference.
- `swmr` (BOOLEAN, named, optional): Open in SWMR read mode (default: `false`)

**Returns:** Table with the same row shape as `h5_tree()`, plus hidden virtual column `filename` (VARCHAR)

**Semantics:**
- the path must resolve to a group, otherwise the function errors
- only immediate children are returned; the requested group itself is not returned
- external groups can be listed if the external target resolves successfully
- row typing, `dtype`, and `shape` are the same as `h5_tree()`
- projected attributes use the same `h5_attr(...)` semantics
- projected output names must be unique under DuckDB's case-insensitive identifier matching and must not collide with
  `path`, `type`, `dtype`, or `shape`

**Examples:**
```sql
-- List the root group's immediate children
SELECT * FROM h5_ls('data.h5');

-- List one specific group
SELECT * FROM h5_ls('data.h5', '/entry/instrument');

-- List the same group across multiple files
SELECT * FROM h5_ls('runs/run_*.h5', '/entry/instrument');

-- Project attributes on the immediate children
SELECT path, NX_class
FROM h5_ls(
    'data.h5',
    '/entry/instrument',
    h5_attr('NX_class', NULL::VARCHAR)
);
```

---

### `h5_read(filename_or_filenames, dataset_path, ...)`

Reads data from one or more datasets in an HDF5 file.

**Parameters:**
- `filename_or_filenames` (VARCHAR or VARCHAR[]): Local path or remote URL to the HDF5 file, a glob
  pattern, or a list of exact filenames/URLs and/or glob patterns. See [Remote Access](#remote-access).
- `dataset_path` (VARCHAR or STRUCT): Dataset path(s) to read. Use `h5_rse()` for run-start encoded columns
- `h5_index()` can be provided to add a virtual index column named `index`
- `h5_alias(name, definition)` can be used to rename a column definition
- Additional dataset paths can be provided (variadic arguments)
- `filename` (BOOLEAN or VARCHAR, named, optional): Add a visible filename column. `true` uses the column name
  `filename`; a string uses the provided column name instead. Without this option, hidden virtual `filename`
  remains available by explicit reference.
- `swmr` (BOOLEAN, named, optional): Open in SWMR read mode (default: `false`)

**Returns:** Table with one column per dataset, plus hidden virtual column `filename` (VARCHAR)

**Column Naming:** Column names are extracted from the last component of the dataset path, for example `/group/data`
becomes `data`. Output names must be unique under DuckDB's case-insensitive identifier matching. Use `h5_alias(...)`
when two dataset paths or generated columns would otherwise collide.

**Scalar Datasets:**
- Scalar (rank-0) datasets are returned as constant columns.
- If all selected datasets are scalar, `h5_read()` returns a single row.
- If any non-scalar dataset is present, scalar columns are broadcast to the row count of the non-scalar datasets.
- If multiple non-scalar regular datasets are selected, the output row count is the minimum outer dimension among them.

**Multi-file Semantics:**
- Rows are concatenated file by file.
- `h5_index()` is the outermost-dimension row index within each matched file.
- Duplicate filename matches are preserved.
- `filename` identifies which file produced each row.
- All matched files must have compatible column definitions.

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

-- Read matching local or SFTP files
SELECT * FROM h5_read('runs/run_*.h5', '/counts');
SELECT * FROM h5_read('sftp://beamline.example.org/data/run_*.h5', '/entry/data');

-- Expand a list of exact files and/or patterns in order
SELECT * FROM h5_read(
    ['runs/calibration.h5', 'runs/run_*.h5'],
    '/counts'
);

-- Materialize filename into SELECT *
SELECT *
FROM h5_read('runs/run_*.h5', '/counts', filename := true);

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

### `h5_attributes(filename_or_filenames, object_path)`

Reads attributes from an object or the file root.

**Parameters:**
- `filename_or_filenames` (VARCHAR or VARCHAR[]): Local path or remote URL to the HDF5 file, a glob
  pattern, or a list of exact filenames/URLs and/or glob patterns. See [Remote Access](#remote-access).
- `object_path` (VARCHAR): Path to the target object, or `/` for root
- `filename` (BOOLEAN or VARCHAR, named, optional): Add a visible filename column. `true` uses the column name
  `filename`; a string uses the provided column name instead. Without this option, hidden virtual `filename`
  remains available by explicit reference.
- `swmr` (BOOLEAN, named, optional): Open in SWMR read mode (default: `false`)

**Returns:** One row per matched file where each non-virtual column represents one attribute
- Column names are the attribute names
- Column types match the attribute types (numeric, string, or arrays)
- If the target object has no attributes, the function raises `IO Error: Object has no attributes: ... in file: ...`
- Invalid UTF-8 string attribute values raise an error in `h5_attributes()`
- Multiple matched files must have the same attribute names, types, and order.
- Attribute output names must be unique under DuckDB's case-insensitive identifier matching.
- Hidden virtual column `filename` (VARCHAR), available by explicit reference.

**Type Support:**
- Numeric scalars and string scalars
- Simple 1D numeric and string arrays, returned as DuckDB `LIST`s
- HDF5 `H5T_ARRAY`-typed attributes are supported when they are 1D
- Empty numeric and string attribute arrays are supported

**Examples:**
```sql
-- Read attributes from a dataset
SELECT * FROM h5_attributes('data.h5', '/measurements');

-- Read the same object's attributes across many files
SELECT filename, units
FROM h5_attributes('runs/run_*.h5', '/measurements');

-- Read specific attributes
SELECT units, description FROM h5_attributes('data.h5', '/temperature');

-- Read attributes from a group
SELECT * FROM h5_attributes('data.h5', '/experiment1');

-- Read file-level (root) attributes
SELECT * FROM h5_attributes('data.h5', '/');

-- Enable SWMR read mode
SELECT * FROM h5_attributes('data.h5', '/measurements', swmr := true);

-- Materialize filename into SELECT *
SELECT *
FROM h5_attributes('runs/run_*.h5', '/measurements', filename := true);
```

---

## Scalar Functions

### `h5_ls(filename, group_path[, projected_attributes...])`

Returns the immediate children of a group as a `MAP(VARCHAR, STRUCT(...))` keyed
by child name. This is the scalar counterpart to the table-valued `h5_ls()`.

**Parameters:**
- `filename` (VARCHAR): File path or remote URL. May vary per row. See [Remote Access](#remote-access).
- `group_path` (VARCHAR): Group path to list. May vary per row.
- `projected_attributes` (variadic, optional): Zero or more `h5_attr(...)` or
  `h5_alias(..., h5_attr(...))` expressions. These must be constant expressions.

**Returns:** `MAP(VARCHAR, STRUCT(path, type, dtype, shape, ...projected attrs...))`

**Semantics:**
- `group_path` must resolve to a group or the function errors
- `filename` is used as a path or URL per input row; lists and glob patterns are not expanded
- `NULL` `filename` yields `NULL`
- `NULL` `group_path` yields `NULL`
- named parameters such as `swmr := true` or `filename := true` are not supported in the scalar form
- projected attributes use the same `h5_attr(...)` semantics as table `h5_ls()`
- projected output names must be unique under DuckDB's case-insensitive identifier matching and must not collide with
  `path`, `type`, `dtype`, or `shape`

**Examples:**
```sql
SELECT h5_ls('data.h5', '/entry/instrument');

WITH groups(path) AS (
    VALUES ('/'), ('/entry/instrument')
)
SELECT path, cardinality(h5_ls('data.h5', path))
FROM groups;

WITH files(filename) AS (
    VALUES ('run_1.h5'), ('run_2.h5')
)
SELECT filename, cardinality(h5_ls(filename, '/entry/instrument'))
FROM files;
```

### `h5_ls_swmr(filename, group_path[, projected_attributes...])`

Scalar variant of `h5_ls()` that forces `swmr = true`.

**Parameters:** Same as scalar `h5_ls()`

**Returns:** Same as scalar `h5_ls()`

**Example:**
```sql
SELECT h5_ls_swmr('data.h5', '/entry/instrument');
```

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

When `h5_read(...)` reads multiple files, `h5_index()` still means the
outermost-dimension row index within the current file, so it starts at `0` for
each matched file.

**Example:**
```sql
SELECT index, measurements FROM h5_read('data.h5', h5_index(), '/measurements');
```

**Predicate pushdown:** range-like filters on the index column (for example `index >= 100`, `index BETWEEN 10 AND 20`,
or comparison-cast forms with bind-time constants) are used to reduce I/O when the planner can normalize them into
simple comparisons.

---

### `h5_attr([name[, default_value]])`

Creates a projected-attribute definition for use with `h5_tree()` or `h5_ls()`.

**Parameters:**
- `name` (VARCHAR, optional): Attribute name to read. Any non-`NULL` bind-time-resolved `VARCHAR` expression is allowed.
- `default_value` (ANY, optional): Bind-time constant default value. Its type becomes the output type of the projected
  column for single-attribute projection. If omitted, `h5_attr(name)` defaults to `NULL::VARIANT`.

**Returns:** STRUCT wrapper used by `h5_tree()` and `h5_ls()`

**Bind-time rules:**
- `h5_attr(name)` is shorthand for `h5_attr(name, NULL::VARIANT)`
- `h5_attr()` defaults to output column name `h5_attr`
- `h5_attr(name)` and `h5_attr(name, default_value)` default to output column name `name`
- `h5_alias(...)` overrides the default output name
- `name` must resolve to a non-`NULL` `VARCHAR` value at bind time
- constant expressions such as `lower('STRING_ATTR')` are allowed
- row-dependent projected-attribute definitions are not allowed
- if `default_value` is provided, it must be a bind-time constant expression with a concrete type
- typed `NULL` defaults such as `NULL::VARCHAR` are allowed
- untyped `NULL` defaults are rejected

**Runtime behavior:**
- `h5_attr()` projects all attributes into one `MAP(VARCHAR, VARIANT)` column named `h5_attr`
- on resolved rows with no attributes, `h5_attr()` produces `{}`
- on unresolved or external rows, `h5_attr()` produces `NULL`
- unsupported attribute values inside `h5_attr()` become `NULL` map entries
- missing attributes use `default_value` or `NULL::VARIANT` when the default is omitted
- present attributes are first converted using the same nominal HDF5-to-DuckDB type rules as `h5_attributes()`
- single-attribute projections are then cast to the type of `default_value`

**String behavior:**
- `VARIANT`-typed or `BLOB`-typed projected scalar string attributes preserve invalid UTF-8 values as `BLOB`
- flexible projected string arrays preserve invalid elements as `BLOB`, including `VARIANT[]`, `VARIANT[N]`,
  `BLOB[]`, and `BLOB[N]` outputs
- text-typed projected attributes still fail on invalid UTF-8 string values

**Supported attribute forms:**
- numeric and string scalars
- simple 1D numeric and string arrays
- 1D `H5T_ARRAY`-typed attributes
- empty numeric and string arrays
- multidimensional attribute dataspaces are not supported

**Example:**
```sql
SELECT path, h5_attr
FROM h5_tree(
    'data.h5',
    h5_attr()
);

SELECT path, type, NX_class
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class')
);

SELECT path, type, NX_class
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class', NULL::VARCHAR)
);
```

---

### `h5_alias(name, definition)`

Renames a column definition when used with `h5_read()` or a projected-attribute
definition when used with `h5_tree()` or `h5_ls()`.

**Parameters:**
- `name` (VARCHAR): Column name to use in the output
- `definition` (VARCHAR or STRUCT): A dataset path, a column definition like `h5_rse()` or `h5_index()`, or a
  projected attribute definition from `h5_attr()`

**Returns:** STRUCT wrapper used by `h5_read()`, `h5_tree()`, and `h5_ls()`

The alias name participates in the same case-insensitive duplicate-name checks as normal output column names.

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
Default SWMR read mode for h5db functions when no explicit `swmr := ...` named
parameter is provided. Defaults to `false`.

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

### HDF5 to DuckDB Type Conversion

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
- **Invalid path**: "IO Error: Failed to open dataset/object: ... in file: ..."
- **Unsupported type**: "IO Error: Unsupported HDF5 type"
- **Invalid RSE data**: "IO Error: RSE run_starts must be strictly increasing: ... in file: ..."
- **No attributes**: "IO Error: Object has no attributes: ... in file: ..."

---

## Performance Notes

- **`h5_read` projection pushdown**: Only reads columns actually used by your query, skipping unused datasets entirely
  for significant performance gains
- **`h5_read` predicate pushdown**: Range-like filters with static constants are applied during scan for RSE and `h5_index()` columns
  to reduce I/O. Supported shapes include `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, bind-time-foldable RHS expressions, and
  comparison-cast forms that normalize to those operators. Unsupported boolean shapes such as `OR`, `!=`,
  `IS DISTINCT FROM`, or expressions like `index + 1 > 10` remain post-scan filters.
- **`h5_tree`/`h5_ls` filename filter pruning**: Filters on the hidden or visible renamed `filename` column can remove
  files from expanded glob/list inputs before the file is opened. This applies only to `h5_tree(...)` and table-valued
  `h5_ls(...)`, not to `h5_read(...)` or `h5_attributes(...)`.
- **Chunked reading**: Data is read in chunks with optimized cache management for memory efficiency
- **Hyperslab selection**: Uses HDF5's hyperslab selection for efficient partial reads
- **RSE optimization**: Run-start encoded data is expanded on-the-fly with O(1) amortized cost per row
- **Parallel scanning**: `h5_read` can scan different row ranges in parallel where the dataset layout and query allow it

---

## Limitations

1. **Compound types** (HDF5 structs) are not currently supported
2. **Enum types** are not currently supported
3. **Opaque, bitfield, reference, time-like, and non-string variable-length HDF5 type classes** are not supported
4. **Datasets with >4 dimensions** are not supported
5. **Multi-dimensional string datasets** are not supported
6. **Attribute multidimensional dataspaces** are not supported (only scalar and 1D)

---

## See Also

- [../README.md](../README.md) - Project overview and quick start
- [RSE_USAGE.md](RSE_USAGE.md) - Detailed guide to run-start encoding
- [developer/DEVELOPER.md](developer/DEVELOPER.md) - Development and building guide
