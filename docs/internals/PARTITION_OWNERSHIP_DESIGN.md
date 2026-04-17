# Larger Logical Partitions For `h5_read` Batch Indexing

## Status

Proposed design.

## Summary

`h5_read` currently implements DuckDB table-function `get_partition_data` by returning the start row of the most
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

- [src/h5_read.cpp](/home/johannes/personal/h5db/src/h5_read.cpp)
- [duckdb/src/parallel/pipeline_executor.cpp](/home/johannes/personal/h5db/duckdb/src/parallel/pipeline_executor.cpp)
- [duckdb/src/execution/operator/persistent/physical_batch_insert.cpp](/home/johannes/personal/h5db/duckdb/src/execution/operator/persistent/physical_batch_insert.cpp)
- [duckdb/src/execution/operator/persistent/physical_batch_copy_to_file.cpp](/home/johannes/personal/h5db/duckdb/src/execution/operator/persistent/physical_batch_copy_to_file.cpp)
- [duckdb/src/execution/operator/helper/physical_buffered_batch_collector.cpp](/home/johannes/personal/h5db/duckdb/src/execution/operator/helper/physical_buffered_batch_collector.cpp)

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

- [benchmark/get_partition_data/bench_get_partition_data.py](/home/johannes/personal/h5db/benchmark/get_partition_data/bench_get_partition_data.py)
- [benchmark/get_partition_data/README.md](/home/johannes/personal/h5db/benchmark/get_partition_data/README.md)

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
