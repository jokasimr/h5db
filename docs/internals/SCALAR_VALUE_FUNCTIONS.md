# Scalar Value Functions

This note describes the implementation shape for scalar
`h5_attributes(...)` and scalar `h5_read(...)`.

## Scalar `h5_attributes`

`h5_attributes(filename, object_path)` is a scalar counterpart to the existing
table function. It returns one value:

```sql
MAP(VARCHAR, VARIANT)
```

The scalar form reads all attributes from exactly one HDF5 object. Attribute
names are map keys and attribute values are stored as `VARIANT`. The return type
is fixed, so `filename` and `object_path` can be arbitrary row expressions.

The implementation shares the same all-attributes map helper used by
`h5_attr()` projection in `h5_tree(...)` and `h5_ls(...)`. This keeps behavior
consistent:

- an object with no attributes returns an empty map
- unsupported attribute values become `NULL` variant map entries
- string attributes are decoded in text-or-blob mode so invalid UTF-8 can be
  represented as `BLOB` inside the variant

The scalar execution path follows scalar `h5_ls(...)`:

- constant `filename` and constant `object_path` produce a constant result vector
- constant `filename` with varying paths opens the file once per vector chunk
- varying filenames are grouped so each distinct file is opened once per vector
  chunk
- `NULL` inputs produce `NULL` outputs

The scalar overload does not expand filename lists or glob patterns. That
matches scalar `h5_ls(...)`: each input row names one file or URL.

## Scalar `h5_read`

The scalar `h5_read(filename, dataset_path)` implementation returns:

```sql
VARIANT
```

Returning `VARIANT` is what makes dynamic filenames and dataset paths feasible.
The scalar function has a fixed DuckDB return type, while the HDF5 dataset type
is discovered at execution time for each row.

The current implementation only supports scalar HDF5 datasets. Non-scalar
datasets fail with a clear error that points users to table
`h5_read(...)` for array-like datasets. This avoids whole-dataset
materialization and keeps the implementation small.

The execution path uses the same vector-shape optimizations as scalar
`h5_ls(...)` and scalar `h5_attributes(...)`:

- constant filename and dataset path: read once per vector chunk and emit a
  constant vector
- constant filename and varying dataset paths: open the file once per vector
  chunk
- varying filenames: group rows by filename and open each distinct file once per
  vector chunk

The scalar read path reuses the existing HDF5-to-DuckDB type conversion
rules from table `h5_read(...)`. It then casts the scalar DuckDB value to
`VARIANT`.

Future support for one-dimensional or multidimensional datasets can extend the
same API by materializing nested `LIST` values inside `VARIANT`, but that is not
part of the current implementation.
