# h5_read Row Alignment Semantics

> Status: Decision recorded. The current `h5_read(...)` behavior is retained:
> when multiple non-scalar regular datasets have different outer dimensions,
> the output row count is the minimum outer dimension. A positional-join-like
> alternative was evaluated but is not implemented.

## Scope

This note documents the row-alignment discussion for `h5_read(...)`.

It covers:

- current row-count semantics
- the proposed positional-join-like alternative
- API consequences of changing the behavior
- implementation areas that would be affected
- tests that would need to change if the proposal is reopened
- known implementation risks and open questions

It does not propose changes to `h5_tree(...)`, `h5_ls(...)`, or
`h5_attributes(...)`.

## Current Behavior

`h5_read(...)` currently treats the selected HDF5 datasets as columns in one
table. For each matched file:

- rank-0 scalar datasets are scalar columns
- rank-1 and higher datasets are regular columns
- regular datasets use their first dimension as the logical row dimension
- if all selected datasets are scalar, the result has one row
- if at least one non-scalar regular dataset is selected, scalar datasets are
  broadcast to that non-scalar row count
- if multiple non-scalar regular datasets are selected, the per-file row count
  is the minimum first-dimension length across them

Example:

```sql
FROM h5_read('file.h5', '/short', '/long');
```

If `/short` has shape `[2]` and `/long` has shape `[5]`, the current result has
two rows. The three trailing values from `/long` are not returned.

This behavior is documented in the public API reference and covered by tests.
The core implementation is in `src/h5_read.cpp`:

- `H5ReadSingleFileBindData::num_rows` stores the per-file row count
- `BindSingleH5ReadFile(...)` tracks the minimum non-scalar regular row count
- `H5ReadSingleFileScan(...)` scans row ranges bounded by that per-file row
  count
- `ScanRegularColumn(...)` assumes every regular column can be read for every
  emitted row

## Why The Current Behavior Is Attractive

The current behavior has a simple invariant:

Every emitted row is backed by every selected non-scalar regular dataset.

That invariant keeps the scan path straightforward:

- no regular column needs trailing null padding
- HDF5 hyperslab reads never cross the dataset's first-dimension extent
- the numeric chunk cache can use the table row count as the column row count
- `h5_index()` is always a valid row index for every non-scalar regular dataset
- RSE validation can use the same derived row count

It also avoids producing unexpectedly large result sets when a small dataset is
selected with a much larger one.

## Weakness Of The Current Behavior

The main problem is silent truncation.

If a user selects a short dataset and a long dataset together, the longer
dataset's trailing rows disappear without an error or a visible marker. This is
especially surprising for exploratory reads, because users may not yet know that
the selected datasets have mismatched lengths.

The behavior also differs from DuckDB's `POSITIONAL JOIN`, where mismatched
inputs are aligned by position and the shorter side is padded with `NULL`.

## Alternative Considered: Outer Positional Alignment

The alternative considered was to make `h5_read(...)` behave like an outer
positional alignment for non-scalar regular datasets.

Under that model, for each matched file:

- the output row count is the maximum first-dimension length across selected
  non-scalar regular datasets
- shorter non-scalar regular datasets produce top-level `NULL` values after
  their physical end
- scalar datasets continue to broadcast to all emitted rows
- all-scalar reads still return one row
- multi-file and glob reads still concatenate per-file results file by file
- `h5_index()` runs from `0` to `max_rows - 1` within each matched file

Example:

```text
/short shape: [2]
/long  shape: [5]
```

would produce:

```text
index  short  long
0      ...    ...
1      ...    ...
2      NULL   ...
3      NULL   ...
4      NULL   ...
```

This is intentionally not a literal copy of DuckDB `POSITIONAL JOIN` for scalar
datasets. A rank-0 HDF5 scalar is a single file/object value, not a one-row
relation. Preserving scalar broadcast is more natural for HDF5 and matches the
existing `h5_read(...)` API.

## API Consequences Of Changing

Changing to outer positional alignment would be a breaking API change.

Visible consequences:

- `COUNT(*)` can increase for mismatched datasets.
- `h5_index()` can expose indexes that are out of range for shorter datasets.
- shorter dataset columns become `NULL` for padded rows.
- `COUNT(short_column)` becomes the way to count physically present rows in a
  shorter dataset.
- `WHERE short_column IS NULL` can now match trailing padded rows.
- `WHERE short_column IS NOT NULL` becomes a practical way to recover the old
  effective row set for that column.
- empty non-scalar datasets no longer necessarily force the whole result to zero
  rows if a longer non-scalar dataset is selected alongside them.
- scalar columns continue to broadcast, so scalar values appear on padded rows.
- aggregates can change when they depend on row count, for example `COUNT(*)`,
  window functions, joins, ordering with `h5_index()`, and downstream positional
  assumptions.

Some aggregates over the longer column may become more complete rather than
truncated. For example, `SUM(long_column)` would include values that are
currently omitted when a shorter column is selected with it.

Multi-file behavior would remain conceptually the same, but each file's local
row count could increase independently. The hidden `filename` column would be
repeated for padded rows because those rows are part of that file's aligned
result.

## Implementation Areas Affected

### Bind-Time Row Count

`BindSingleH5ReadFile(...)` currently tracks `min_rows` across non-scalar
regular datasets. The proposed behavior would change this to `max_rows`.

The bind result would still need a per-file `num_rows`, but the meaning would
be:

- all scalar: `1`
- any non-scalar regular dataset: maximum first-dimension length

The implementation should also make the per-column row count explicit, either
with a helper such as `RegularRowCount(spec)` or a stored field in
`RegularColumnSpec`. Today the information is available as `spec.dims[0]`, but
making it explicit would reduce mistakes in the scan path.

### Regular Dataset Scanning

`ScanRegularColumn(...)` currently assumes the requested output range is fully
readable from the HDF5 dataset.

With outer positional alignment it would need to split each requested range:

```text
requested output range: [position, position + to_read)
physical dataset range: [0, spec_row_count)

readable prefix: overlap with physical dataset range
null suffix: rows beyond spec_row_count
```

The scan behavior would become:

- if `position >= spec_row_count`, emit a constant `NULL` vector
- otherwise read `min(to_read, spec_row_count - position)` rows from HDF5
- if the requested output chunk extends past `spec_row_count`, mark the trailing
  rows as top-level `NULL`

For multidimensional datasets, the padded value must be a top-level `NULL`
array value, not an array containing null elements.

### Numeric Chunk Cache

The numeric chunk cache is the most sensitive implementation area.

Today cache sizing and refresh use the file's `num_rows`, which is also every
regular dataset's readable row bound under minimum-row semantics. With
max-row semantics that is no longer true.

The cache would need column-local bounds:

- `ComputeChunkSize(...)` should be called with the regular column's row count,
  not the file's aligned row count.
- `TryLoadChunks(...)` must never request a range starting at or extending past
  the physical dataset extent.
- `ScanRegularColumn(...)` must only wait for cached data up to the readable
  prefix, not the full requested output chunk.
- cache copy logic must leave the padded suffix null.

This area needs focused tests because an incorrect implementation can read past
the HDF5 dataspace or wait forever for cache data that can never be loaded.

### String Dataset Scanning

String columns bypass the numeric chunk cache and read directly. They still need
the same readable-prefix/null-suffix split.

Care is needed for zero-row readable prefixes:

- do not call HDF5 read helpers for zero rows
- emit constant `NULL` when the entire requested output chunk is beyond the
  dataset extent
- mark only the trailing suffix null when the chunk straddles the dataset end

### RSE Columns

RSE columns currently require at least one non-scalar regular dataset to
determine total row count. That requirement would remain.

Under max-row semantics, RSE validation and scan output would use the max
regular row count. This means:

- `run_starts.back() >= max_rows` remains invalid
- an empty RSE still emits `NULL` for every aligned row
- if the last RSE run starts before `max_rows`, it extends to `max_rows`

This is internally consistent but semantically more visible than today: a long
regular dataset can extend the logical duration of an RSE column.

### Filter Pushdown And Row Ranges

The current pushed row-range logic is primarily relevant for `h5_index()` and
RSE filters. Those ranges are built against `bind_data.num_rows`.

With max-row semantics:

- index filters naturally use the aligned max row count
- RSE filters use the aligned max row count
- regular-column filters are still applied by DuckDB after scanning
- padded regular-column `NULL`s should therefore be filtered by normal DuckDB
  null semantics

If regular-column filter pushdown is added later, it must explicitly account for
the padded null suffix.

### Statistics

`Cardinality`/statistics returned for `h5_read(...)` currently use total
per-file `num_rows`. Changing per-file `num_rows` from min to max would update
those estimates naturally, but tests or plans that assert cardinality may need
adjustment.

### Documentation

The public API docs would need to replace the minimum-row statement with
outer-positional semantics and explain scalar broadcast.

A migration note would be useful because the change is easy to miss in queries
that only inspect aggregates.

## Test Changes If Reopened

If this change is implemented later, tests should be adjusted as a coherent
behavioral migration rather than by only changing expected counts.

Existing tests to update:

- `test/sql/h5db.test`
  - the multi-dataset test using `/integers`, `/floats`, and `/strings` should
    expect the long integer/floating rows to remain visible
  - a query that currently expects the minimum row count should instead check
    `COUNT(*)`, `COUNT(strings)`, and trailing `strings IS NULL`
- `test/sql/empty_scalar.test`
  - `/short` plus `/long` should return the long row count
  - `/empty_1d` plus `/long` should return the long row count with the empty
    column all `NULL`
  - scalar plus non-scalar should continue to prove scalar broadcast
- `test/sql/multidim_mismatch.test`
  - count should become the max first dimension
  - add checks that the shorter multidimensional array column is top-level
    `NULL` on padded rows
- any tests that describe "minimum row count" should be renamed to avoid
  preserving the old mental model

New focused tests to add:

- 1D numeric short + long:
  - `COUNT(*) = max`
  - `COUNT(short) = physical_short_len`
  - trailing rows have `short IS NULL`
- string short + long:
  - string read path pads trailing rows correctly
- multidimensional short + long:
  - padded rows are top-level `NULL` arrays
  - non-padded rows still have the expected nested array values
- empty non-scalar + long:
  - result row count follows the long dataset
  - empty column has zero non-null values
- scalar + mismatched non-scalars:
  - scalar broadcasts across both physical and padded rows
- `h5_index()`:
  - index reaches `max_rows - 1`
  - index filters over padded rows work
- projection pushdown:
  - selecting only the shorter column from `h5_read(file, short, long)` still
    returns the max row count, because row count is determined by the bound
    function inputs rather than projected columns
- numeric cache boundary:
  - a chunk straddles the shorter dataset end
  - the readable prefix is returned and the suffix is null
- all-padded numeric cache chunk:
  - a requested chunk starts after the shorter dataset end and does not hang
- RSE with mismatched regular datasets:
  - RSE uses max aligned row count
  - empty RSE emits nulls across max aligned rows
  - invalid `run_starts` are validated against max aligned rows
- multi-file/glob:
  - each file aligns independently before concatenation
  - `filename` is present on padded rows

Remote and SFTP rewritten test suites should not need separate semantic cases if
the SQLLogicTest rewrites cover the updated local tests. They remain useful for
catching cache/read-boundary mistakes on non-local VFDs.

## Implementation Problems And Risks

### Cache Reads Past Dataset End

The numeric cache is optimized around the current invariant that every regular
column has the file's row count. Changing row-count semantics invalidates that
assumption.

The highest-risk bug is an HDF5 read that requests rows beyond a shorter
dataset's extent.

### Cache Wait Deadlock

The scanner currently waits until cached chunks cover the requested read range.
For a shorter column, padded rows will never be cached. The wait condition must
be based on the readable prefix, not the requested aligned output range.

### Incorrect Null Representation For Arrays

For multidimensional datasets, trailing padded rows should be `NULL` at the
column value level. They should not be arrays filled with nulls.

This distinction matters for expressions such as:

```sql
WHERE array_column IS NULL
```

and for functions that inspect array length or elements.

### Larger Result Sets

Queries that currently return a small truncated result can become very large.
For example, selecting one 10-row dataset with one 10-million-row dataset would
return 10 million rows.

This is semantically consistent with outer positional alignment, but it can be a
large practical change for interactive use and remote reads.

### RSE Duration Ambiguity

RSE columns depend on a regular dataset to provide total row count. With max-row
semantics, an unrelated long regular dataset can extend the final RSE run or the
empty-RSE null region.

That is consistent with the proposed rule but may be surprising.

### Migration Risk

The old behavior can hide data by truncation; the new behavior can expose more
rows. Both can surprise users in different ways.

Queries most likely to change are those using:

- `COUNT(*)`
- joins against the `h5_read(...)` result
- window functions
- `LIMIT` without explicit ordering
- `h5_index()`
- filters that assume all selected columns are present on every row

## Open Questions

The current decision is to keep existing behavior, so these questions only
matter if the change is reopened.

1. Should an empty non-scalar dataset plus a scalar return zero rows or one row?
   The natural consequence of "any non-scalar controls row count" is zero rows,
   but this should be explicit in docs and tests.

2. Should RSE get an explicit row-count source in the future? Today it derives
   row count from selected regular datasets. Outer alignment would make that
   dependency more visible.

3. Should the docs call the future behavior "positional join semantics" or
   "outer positional alignment"? The latter is more precise because scalar
   broadcasting intentionally differs from a literal positional join.

4. Should there be a warning or migration guide if the behavior changes in a
   future release? A migration note is probably warranted because the change can
   affect aggregates without causing errors.

5. Should regular-column filter pushdown, if implemented later, treat padded
   null suffixes as an explicit scan range? Current regular filters are handled
   after scanning, but future pushdown would need a clear null-padding model.

6. Should file-level schema compatibility include row-count compatibility in any
   optional strict mode? The current decision is no strict mode and no semantic
   change, but this question may return if users ask for mismatch detection
   rather than truncation or null padding.

## Current Decision

Keep the current minimum-row-count behavior.

Rationale:

- it preserves existing API behavior
- it avoids a breaking result-cardinality change
- it keeps the scanner and numeric cache invariants simple
- it avoids introducing large padded result sets unexpectedly

The positional-alignment alternative remains a plausible future breaking
change, but it should be implemented only with focused cache-boundary tests,
string/null-padding tests, multidimensional null tests, RSE tests, and clear
release notes.
