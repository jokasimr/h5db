# Archived: Larger Logical Partitions For `h5_read` Batch Indexing

## Status

Archived historical design note. This document does not describe the current
`h5_read` implementation.

`h5_read` previously registered DuckDB `get_partition_data` using logical
partitions owned by local scan states. That implementation has been removed.
The current implementation does not register `get_partition_data`, because the
old ownership model could leave a thread outside `Scan()` while its abandoned
partition still blocked shared cache progress for later ranges.

The old `partition_ownership` regression tests were replaced by
`cache_progress` tests that preserve useful boundary and ordered-sink coverage
without asserting the removed batch-index behavior.

Keep this file only as background for future work on a replacement design. Any
new implementation must avoid coupling shared cache progress to a thread
returning to `Scan()` after producing a chunk.

The rest of this note is preserved mostly as written. Phrases such as "current
implementation" below refer to the removed partition-based implementation, not
to the current source tree.

## Why the previous design was removed

The failed design coupled two mechanisms that had different lifetime
requirements:

- DuckDB batch-index ownership: a local scan state claimed a logical partition
  and `get_partition_data` kept reporting that partition until the local state
  exhausted it.
- h5_read cache progress: numeric regular columns used a small shared cache that
  could only advance once all earlier claimed row ranges had been returned or
  skipped.

That coupling only works if DuckDB keeps calling `Scan()` on every local state
until its owned partition is exhausted. DuckDB does not promise that. A plan may
stop consuming a source after a scan call has returned a chunk, for example due
to `LIMIT`, order-preserving collection, a cross product that already has enough
rows, cancellation, or a downstream error.

The failure sequence was:

1. A thread claimed a logical partition and returned the first chunk from that
   partition.
2. The query stopped calling `Scan()` on that local state before the rest of the
   partition was returned or skipped.
3. The unvisited tail of the partition kept `position_done` behind the cache
   refresh point.
4. Other threads had already claimed later ranges and needed cache windows past
   the current shared cache.
5. Cache refresh could not advance, because the oldest incomplete range belonged
   to a local state that might never re-enter `Scan()`.

This was not primarily a lost wakeup in `someone_is_fetching`. Waiting threads
could be notified correctly and still be stuck, because the condition required
for shared cache progress had become impossible: the thread that owned the
missing earlier work had left the scan path.

## Alternatives considered

### Add a direct-read fallback

Another option was to let a thread bypass the shared cache and read directly
when its requested range is outside the cache windows and cache progress is
blocked.

That would avoid some hangs, but it hides the broken ownership invariant rather
than fixing it. It also risks duplicate reads, worse performance on remote or
chunked inputs, and a more complex set of correctness paths: cached read,
direct read, and transitions between them.

### Make chunk buffers thread-local

A larger refactor would give each local scan state its own chunk buffers. That
removes most shared-cache coordination, and a thread that leaves `Scan()` no
longer owns shared cache progress.

The downside is bounded-memory behavior. With large HDF5 chunks and many DuckDB
threads, one chunk buffer per thread per projected column can become expensive.
It can also delay parallel startup: later threads may wait while earlier threads
fetch their first chunks. A production version would likely need the buffers to
be managed by DuckDB's buffer manager so memory pressure can evict or spill
them. That is a larger design than was needed for the immediate correctness
fix.

### Add small per-thread caches backed by the shared cache

We also considered giving each thread a small local cache, roughly one
partition-sized window, and using the shared cache only to fill that local cache.
That would avoid a thread holding a shared-cache partition after returning a
chunk.

The problem is that the shared cache can still be unable to provide the needed
window unless there is a fallback path or a more general cache design. With
fallbacks, the implementation again risks duplicate reads and multiple
correctness paths. Without fallbacks, it can recreate the same no-progress
condition in a different form.

### Replace the two-window cache with a bounded keyed shared cache

A more general shared cache could be keyed by dataset and row/chunk range, with
bounded memory, waiters for in-flight fetches, and eviction independent of scan
order. That is likely a better long-term direction if h5_read needs both maximum
scan performance and strict memory bounds.

The drawback is complexity. The implementation needs correct eviction,
refcounting or pinning, duplicate-read prevention, and careful behavior under
early stop, cancellation, and errors. If it still needs direct-read fallback
under pressure, it inherits some drawbacks of the fallback approach.

### Remove `get_partition_data` and logical partition ownership

The chosen fix was to remove the `get_partition_data` registration and the
logical partition ownership mechanism.

This is the smallest robust fix because no local scan state owns a larger
logical partition across `Scan()` calls. Once a scan call returns a chunk, the
source no longer depends on that local state returning later to release a batch
index or unblock cache progress. The shared cache still has coordination, but it
is no longer coupled to abandoned logical partitions.

The tradeoff is that ordered sinks no longer get h5_read-specific batch indexes,
so some `CTAS`, `INSERT`, `COPY`, and order-preserving result paths may use less
parallel ordered-sink machinery than the partitioned design intended. That is a
performance tradeoff, not a correctness risk. The corresponding tests were
renamed from `partition_ownership` to `cache_progress` to keep boundary and
ordered-sink coverage without asserting removed batch-index behavior.

This decision is intentionally conservative. A future `get_partition_data`
implementation should be treated as a new design, not as a small restoration of
the removed partition-ownership state machine.

## Summary

At the time this note was written, `h5_read` implemented DuckDB table-function `get_partition_data` by returning the start row of the most
recent scan chunk. In practice that means one logical batch transition per output `DataChunk`, which is usually one
transition per `STANDARD_VECTOR_SIZE` rows.

That implementation is correct, and it already enables better DuckDB plans for ordered sinks such as:

- `BATCH_CREATE_TABLE_AS`
- `BATCH_INSERT`
- `BATCH_COPY_TO_FILE`

However, profiling shows that the current logical partition size is too small for those ordered sink paths. The cost
is not in `get_partition_data` itself. The cost is in DuckDB's sink-side `NextBatch` handling, which runs once for
every logical batch transition.

This document proposes changing `h5_read` from "one partition per output chunk" to "one owned partition per thread,
many output chunks per partition".

The core idea is:

- a thread claims exclusive ownership of a larger logical row partition
- the thread still emits normal vector-sized scan chunks
- `get_partition_data` keeps returning the same logical partition ordinal until the thread exhausts that partition
- DuckDB therefore calls sink `NextBatch` much less often

## Background

### How DuckDB uses `get_partition_data`

DuckDB source operators can implement `get_partition_data` to provide partition metadata for sink operators that
require ordering information.

The important execution path is:

1. `PhysicalTableScan` checks whether the table function supports partitioning and forwards the callback.
2. `PipelineExecutor::NextBatch` calls `source->GetPartitionData(...)`.
3. DuckDB compares the returned batch index to the previous batch index.
4. If the batch changed, DuckDB calls `sink->NextBatch(...)`.

The main consumers relevant to `h5_read` are:

- ordered bulk-write sinks:
  - `PhysicalBatchInsert`
  - `PhysicalBatchCopyToFile`
- ordered result collection:
  - `PhysicalBufferedBatchCollector`
- order-preserving `LIMIT`

This means `get_partition_data` affects more than `INSERT`/`COPY`; it can also affect plain `SELECT` and `LIMIT`
when DuckDB chooses order-preserving batch-aware collectors.

### How `h5_read` currently implements it

In the current code:

- `H5ReadScan` claims the next row range and stores its start row in local state
- `H5ReadGetPartitionData` returns that start row as the batch index

Relevant code:

- [src/h5_read.cpp](../../src/h5_read.cpp)
- [duckdb/src/parallel/pipeline_executor.cpp](../../duckdb/src/parallel/pipeline_executor.cpp)
- [duckdb/src/execution/operator/persistent/physical_batch_insert.cpp](../../duckdb/src/execution/operator/persistent/physical_batch_insert.cpp)
- [duckdb/src/execution/operator/persistent/physical_batch_copy_to_file.cpp](../../duckdb/src/execution/operator/persistent/physical_batch_copy_to_file.cpp)
- [duckdb/src/execution/operator/helper/physical_buffered_batch_collector.cpp](../../duckdb/src/execution/operator/helper/physical_buffered_batch_collector.cpp)

This implementation is semantically correct because:

- claimed row ranges are monotone
- different threads claim disjoint ranges
- batch indexes never move backward

## Motivation

### Profiling result

Temporary counters were added around DuckDB `PipelineExecutor::NextBatch` to measure:

- number of batch transitions
- time spent in `source->GetPartitionData(...)`
- time spent in `sink->NextBatch(...)`
- source rows represented by each transition

On the 10M-row `large_simple.h5:/integers` benchmark with `threads=8`:

- `CTAS`
  - `next_batch_calls = 4891`
  - `avg_source_rows = 2047.92`
  - `get_partition_ms = 0.322`
  - `sink_next_batch_ms = 450.875`
  - wall time about `0.12s`
- `COPY`
  - `next_batch_calls = 4891`
  - `avg_source_rows = 2047.92`
  - `get_partition_ms = 0.312`
  - `sink_next_batch_ms = 363.032`
  - wall time about `0.20s`

For `LIMIT` and plain `SELECT`, transition overhead was tiny.

### What this means

The important conclusion is:

- `get_partition_data` is not the expensive part
- per-batch sink transition work is expensive on ordered sink paths
- the current logical partition size is too small for `CTAS`/`INSERT`/`COPY`

The per-transition sink work is roughly:

- `CTAS`: about `92 us`
- `COPY`: about `74 us`

This is large enough that reducing transition count is a meaningful optimization target.

## Goals

- Reduce batch transition frequency for ordered sink paths.
- Keep scan output chunk size at normal DuckDB vector size.
- Preserve current correctness guarantees for:
  - parallel scan ordering
  - predicate pushdown with sparse row ranges
  - ordered sink behavior
- Avoid coupling logical partition size to cache chunk size.
- Avoid changing query semantics or plan eligibility.

## Non-Goals

- Do not redesign DuckDB sink operators.
- Do not change `h5_read` cache chunk sizing as part of this work.
- Do not introduce larger-than-vector `DataChunk`s.
- Do not change the meaning of `h5db_batch_size`.
- Do not make partition sizing depend on specific sink classes unless DuckDB exposes that information cleanly.

## Proposed Design

### Core idea

Introduce a second granularity level:

- `scan_batch_size`
  - how many rows one `H5ReadScan` call returns
  - remains bounded by `STANDARD_VECTOR_SIZE`
- `logical_partition_size`
  - how many rows one thread owns for ordered partitioning
  - larger than `scan_batch_size`

Within one owned logical partition, a thread may perform multiple scan calls, but all of those scan calls report the
same batch index via `get_partition_data`.

### Ownership model

Each thread owns at most one logical partition at a time.

The local state should mirror the existing global-state concepts and naming:

- global `position`
  - next globally unclaimed row
- local `position`
  - next locally unread row within the currently owned partition
- local `position_end`
  - exclusive end of the currently owned partition

Minimal local state:

- `position`
- `position_end`

Global state keeps a monotone allocator for logical partitions:

- `position`
- fixed or computed `logical_partition_size`

No other local partition state is required.

The owned partition start is derivable from local state:

- `partition_start = position_end - logical_partition_size`

This works cleanly if partition ownership is represented using a fixed logical span, even for the final partition.
In that model:

- a claimed partition is always `[start, start + logical_partition_size)`
- local `position_end` stores `start + logical_partition_size`
- scan and range selection clamp to the actual dataset end

Likewise, an explicit "has owned partition" flag is unnecessary:

- no owned partition iff `position == position_end`
- owned partition active iff `position < position_end`

So the minimal local-state model is:

- `position`
  - current local scan cursor
- `position_end`
  - exclusive end of the currently owned logical partition

### Scan behavior

When a thread enters `H5ReadScan`:

1. If `position == position_end`, it claims the next partition starting at `start`.
2. It sets:
   - `position = first valid row within that partition`
   - `position_end = start + logical_partition_size`
3. The thread scans within `[position, min(position_end, num_rows))`.
4. Each scan call returns up to `scan_batch_size` rows.
5. After producing rows, `position` advances to the end of the emitted range.
6. When `position == position_end`, the local partition is exhausted and the next call claims a new partition.

This preserves:

- vector-sized output
- exclusive partition ownership
- monotone batch indexes

### Batch index representation

The batch index should not be the partition start row. It should be the dense logical partition ordinal.

With a fixed logical partition size `P`:

- partition `0` owns rows `[0, P)`
- partition `1` owns rows `[P, 2P)`
- partition `2` owns rows `[2P, 3P)`

Then the batch index is:

- `batch_index = partition_start / P`

or equivalently from minimal local state:

- `batch_index = position_end / P - 1`

if `position_end` is always assigned as `partition_start + P`.

This has two advantages:

- it still gives DuckDB a monotone ordering token
- it removes the current effective `< 1e13 rows` limit from `h5_read` batch-indexed sink paths, because DuckDB's
  `1e13` limit now applies to partitions rather than individual rows

This is the preferred representation even if the current row-start-based implementation is technically correct.

### Sparse pushdown behavior

Partitions are defined over logical row space, not over "number of matching rows".

If predicate pushdown produces sparse valid row ranges, a thread still owns a fixed row interval, but it may emit:

- many chunks
- one chunk
- zero chunks

within that interval.

This is acceptable as long as:

- ownership is exclusive
- the thread only reports the partition start derived from `position_end - logical_partition_size` when it actually
  emits rows
- empty partitions are skipped cleanly

Implementation detail:

- partition claim should not itself depend on `valid_row_ranges`
- chunk selection inside a partition should use `NextRangeFrom(...)` bounded by `position_end`

This keeps the ownership model simple and avoids coupling partition assignment to filter density.

### Why this is safe for DuckDB sinks

DuckDB ordered sinks assume that one batch index corresponds to one logical ordered batch. They do not assume that a
batch contains exactly one source vector.

What must remain true is:

- different threads must not concurrently emit rows for the same batch index
- batch indexes must be monotone
- batch indexes must remain below DuckDB's per-pipeline limit

Exclusive partition ownership preserves those requirements.

### Why this should not be tied to the cache

Logical partition size and cache chunk size solve different problems:

- cache chunk size controls source-side IO/cache behavior
- logical partition size controls sink-side batch transition frequency

They should remain independent. Matching them would create unnecessary coupling and would likely make the cache policy
harder to reason about.

## Suggested Default

Start with:

- `logical_partition_size = 8 * STANDARD_VECTOR_SIZE`

or

- `logical_partition_size = 10 * STANDARD_VECTOR_SIZE`

Rationale:

- this should reduce `NextBatch` calls by roughly `8x` to `10x`
- it still leaves hundreds of partitions on multi-million-row scans
- it is small enough that load imbalance risk stays limited

Do not start at `100k` rows by default. A moderate first step is easier to evaluate and safer on sparse inputs.

## Expected Outcomes

### Expected wins

Ordered sink shapes should improve:

- `CTAS`
- `INSERT INTO ... SELECT`
- `COPY (SELECT ...) TO ...`

The direct effect should be:

- fewer `PipelineExecutor::NextBatch` calls
- fewer sink `NextBatch` calls
- lower aggregate CPU in sink transition handling

### Expected neutral cases

These shapes should see little change:

- plain order-preserving `SELECT`
- order-preserving `LIMIT`

Profiling already shows their batch-transition overhead is small.

### Expected risks

- Slightly worse load balancing on sparse pushdown scans.
- Larger "unit of ownership" may delay batch completion slightly.
- More local state in `H5ReadLocalState`.

These are acceptable if the state remains minimal and the measured sink win is real.

## Benchmark Acceptance Criteria

Use the existing harness:

- [benchmark/get_partition_data/bench_get_partition_data.py](../../benchmark/get_partition_data/bench_get_partition_data.py)
- [benchmark/get_partition_data/README.md](../../benchmark/get_partition_data/README.md)

### Required measurements

Compare:

- current implementation
- new owned-partition implementation

Run with:

- `threads=1`
- `threads=8`

Use at least:

- `ctas_large_simple`
- `insert_large_simple`
- `copy_large_simple_csv`
- `copy_pushdown_large`
- `ctas_sparse_pushdown`
- `limit_filter_large_simple`
- `select_stream_large_regular`
- `select_stream_nd_chunked`
- `union_all_ctas_multithreading`

### Success criteria

The implementation should satisfy all of the following:

1. Ordered sink transition count drops substantially on large scans.
   Target: at least `8x` fewer `NextBatch` calls on `ctas_large_simple` and `copy_large_simple_csv`.

2. Ordered sink transition CPU drops substantially.
   Target: at least `5x` reduction in aggregate `sink_next_batch` CPU for `ctas_large_simple` and `copy_large_simple_csv`.

3. Ordered sink wall time improves measurably at `threads=8`.
   Target:
   - `CTAS`: at least `15%` improvement
   - `COPY`: at least `10%` improvement
   - `INSERT`: at least `10%` improvement

4. Single-thread performance does not regress materially.
   Target: no more than `5%` regression at `threads=1` on the ordered sink scenarios.

5. Non-sink query shapes do not regress materially.
   Target: no more than `5%` regression on:
   - `limit_filter_large_simple`
   - `select_stream_large_regular`
   - `select_stream_nd_chunked`

6. Sparse pushdown behavior remains acceptable.
   Target: no more than `10%` regression on:
   - `copy_pushdown_large`
   - `ctas_sparse_pushdown`

If the implementation reduces `NextBatch` counts but fails the wall-time criteria, the design should be reconsidered
before merging.

## Test Plan

### Correctness tests

Add SQLLogicTests that explicitly exercise partition ownership boundaries.

Required cases:

- row count less than one logical partition
- row count exactly one logical partition
- row count one logical partition plus one row
- row count exactly two logical partitions
- row count two logical partitions plus one row
- partition ownership with `scan_batch_size < logical_partition_size`

These should use regular numeric datasets so the expected row order is easy to assert.

### Sparse pushdown tests

Add cases where valid row ranges:

- fall entirely within one partition
- cross a partition boundary
- produce a partial final chunk near a partition boundary
- produce multiple disjoint ranges inside one partition

These should verify:

- row correctness
- row ordering
- no duplicates
- no missed rows

### Ordered sink runtime tests

Add runtime tests, not just plan tests, for:

- `CTAS`
- `INSERT INTO ... SELECT`
- `COPY (SELECT ...) TO ...`

The tests should verify:

- full row counts
- correct row order
- correct behavior with projection pushdown
- correct behavior with sparse pushdown

### Concurrency stress tests

Run:

- `make test`
- repeated multi-run stress on the new partition tests

Recommended stress shape:

- several concurrent `unittest` invocations
- repeated ordered sink queries over the same fixture

The goal is to catch:

- ownership bugs
- duplicate partition assignment
- skipped partitions
- hangs caused by incorrect scan-progress accounting

## Implementation Notes

### State and invariants

The design should preserve a small invariant set.

Prefer:

- one global monotone partition allocator
- one owned partition per local state
- only the minimal local state required to represent that ownership
- naming that mirrors the existing global state

Avoid:

- multiple overlapping notions of "current batch"
- storing redundant synchronized state that can drift apart

Recommended local invariants:

- `position <= position_end`
- active owned partition iff `position < position_end`
- no active owned partition iff `position == position_end`
- any emitted row range must satisfy:
  - `position_end - logical_partition_size <= emitted_start`
  - `emitted_end <= position_end`

### `position_done` and completed range handling

The current scan tracks completion using actual returned row ranges, not just claimed work.

That should remain true.

Logical partition ownership must not be confused with:

- rows already emitted
- rows already skipped by filtering
- rows fully completed across all threads

Only actual scan output progress should feed the completion bookkeeping.

### DuckDB batch-index limit

DuckDB requires per-pipeline batch indexes to stay below `PipelineBuildState::BATCH_INCREMENT`.

The proposed design should use dense logical partition ordinals as batch indexes, not row starts. That keeps the same
DuckDB limit in principle, but applies it to partition count rather than row count, which is effectively unbounded for
realistic `h5_read` scans.

## Rollout Plan

1. Implement the owned-partition state machine with a fixed moderate partition size.
2. Keep scan chunk sizing and cache sizing unchanged.
3. Add the correctness and sparse pushdown tests first.
4. Benchmark with the existing harness.
5. Profile `NextBatch` again to confirm that:
   - transition count dropped as expected
   - sink transition CPU dropped proportionally
6. Only then consider tuning the default partition size further.

## Open Questions

- Should partition size be a fixed multiple of `STANDARD_VECTOR_SIZE`, or derived from projected row width?
- Should sparse pushdown scans use a smaller partition size than dense scans?
- Is there any DuckDB-level API extension worth proposing so sources can know whether the consumer is an ordered bulk
  sink versus a buffered collector?

These questions should be revisited only after the fixed-size owned-partition prototype is benchmarked.
