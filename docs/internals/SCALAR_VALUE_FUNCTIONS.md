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

Scalar HDF5 datasets become primitive variant values. Non-scalar simple
dataspaces are fully materialized as nested arrays inside the variant, with one
nested level per HDF5 dimension. Empty dimensions are supported, and null
dataspaces continue to produce SQL `NULL`.

`h5db_scalar_read_memory_limit` guards this materialization and defaults to
`64MB`. The conservative estimate covers the typed source, nested container and
variant metadata, the encoded payload, the copy into the result vector, and
values already retained in the current output chunk. Numeric and fixed-length
string estimates are available from the dataspace and type. Variable-length
strings first use an element-count lower bound, then charge their decoded
payload size after HDF5 has read them and before `VARIANT` encoding. HDF5's
string payloads and internal I/O, conversion, and global-heap buffers have
already been allocated at that point, so the guard is best-effort for
variable-length strings rather than a hard process RSS limit. `none` explicitly
disables the guard. Callers should use table `h5_read(...)` for streaming large
datasets.

The scalar guard is independent of DuckDB's `memory_limit`, which governs the
buffer manager rather than vectors and query results. It covers only the
materialization performed by scalar `h5_read`; operations on the returned
variant, including casts, can allocate additional memory.

As with any schema-less array value, dimensions nested below an empty dimension
cannot be recovered from the variant alone because there is no child value in
which to encode them. Callers that know the expected schema can still cast the
result to the corresponding nested list type.

The execution path uses the same vector-shape optimizations as scalar
`h5_ls(...)` and scalar `h5_attributes(...)`:

- constant filename and dataset path: read once per vector chunk and emit a
  constant vector
- constant filename and varying dataset paths: open the file once per vector
  chunk
- varying filenames: group rows by filename and open each distinct file once per
  vector chunk

The scalar read path reuses the existing HDF5-to-DuckDB type conversion rules
from table `h5_read(...)`. Every supported dataset allocates a one-row,
runtime-typed vector; non-scalar datasets use nested `LIST` vectors. Numeric
data is read in one `H5Dread` call directly into its contiguous leaf storage.
Strings require an HDF5-compatible fixed-width buffer or variable-length
pointer array for decoding, after which each value is copied into the typed
`VARCHAR` leaf vector. DuckDB's vector cast then encodes the typed vector as
`VARIANT`, which is copied into the requested output row. This avoids a much
larger recursive `Value` tree for both numeric and string arrays.

HDF5 cannot use the final output `VARIANT` as its destination. It is represented
by type/offset metadata, child relationships, and a per-row encoded blob rather
than a homogeneous leaf buffer. The temporary typed vector provides that leaf
buffer while leaving DuckDB's variant encoder responsible for its internal
representation. The dataset's element type and rank are discovered at runtime
and may differ from one input row to the next.
