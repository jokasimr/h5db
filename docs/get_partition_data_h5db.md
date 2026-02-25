# get_partition_data API Investigation (DuckDB) and H5DB Implications

This document explains how DuckDB’s `get_partition_data` table-function callback is used, what it returns, and what it would mean to implement it for H5DB’s HDF5 reader.

## What the API is
- **Callback**: `TableFunction::get_partition_data` (type `table_function_get_partition_data_t`)
- **Signature**: `OperatorPartitionData (*)(ClientContext &, TableFunctionGetPartitionInput &)`
- **Return type**: `OperatorPartitionData`
  - `batch_index` (required if batch indexing is requested)
  - optional `partition_data` for partition columns (not used by table scans/Arrow)

References:
- `duckdb/src/include/duckdb/function/table_function.hpp`
- `duckdb/src/include/duckdb/execution/partition_info.hpp`

## Where DuckDB uses it
- **PhysicalTableScan** calls it only if the function supports partitioning.  
  - `PhysicalTableScan::SupportsPartitioning` returns false if `get_partition_data` is unset.  
  - `PhysicalTableScan::GetPartitionData` forwards to the table function callback.  
  - `duckdb/src/execution/operator/scan/physical_table_scan.cpp`

- **PipelineExecutor::NextBatch** uses the returned `batch_index` to move between sink batches and to enforce monotonicity:
  - If a new chunk returns a **lower batch index** than previously seen in that local sink state, DuckDB raises an internal error.  
  - `duckdb/src/parallel/pipeline_executor.cpp`

This batch index is a key input to ordering-sensitive sinks (e.g., batch copy/insert) that need to preserve insertion order while running in parallel.

## Concrete planning paths that benefit from get_partition_data
- **COPY TO FILE (CSV/Parquet)**  
  - Planner chooses batch copy only when batch indexes are supported: `duckdb/src/execution/physical_plan/plan_copy_to_file.cpp`.  
  - CSV: `duckdb/src/function/table/copy_csv.cpp` (`WriteCSVExecutionMode`).  
  - Parquet: `duckdb/extension/parquet/parquet_extension.cpp` (`ParquetWriteExecutionMode`).  
  - Batched sink uses `partition_info.batch_index`: `duckdb/src/execution/operator/persistent/physical_batch_copy_to_file.cpp`.
- **INSERT INTO / CREATE TABLE AS SELECT**  
  - Batch insert used only if sources support batch indexes:  
    `duckdb/src/execution/physical_plan/plan_insert.cpp`, `duckdb/src/execution/physical_plan/plan_create_table.cpp`.  
  - Batched sink uses `partition_info.batch_index`: `duckdb/src/execution/operator/persistent/physical_batch_insert.cpp`.

## How h5_read actually scans today
- **Range assignment is global and monotonic**: `H5ReadGlobalState::position` is shared; `GetNextDataRange` advances it under a mutex, so every assigned range has a higher `position` than the last assigned range.
- **Ranges respect pushdown filters**: `valid_row_ranges` is built in `H5ReadInit` from RSE/index filters and intersected; `NextRangeFrom` only returns rows within those ranges.
- **Chunk size and alignment**: each scan gets up to `STANDARD_VECTOR_SIZE` rows starting at the assigned `position` (or the next valid range start).
- **Completion can be out of order**: threads may finish in a different order than assignment. `position_done` and `completed_ranges` track completed ranges to drive cache refresh, not output ordering.
- **No local state**: `h5_read` only uses a global state; there is no per-thread/local state today.

## Examples in DuckDB
- **Arrow scan**: returns `OperatorPartitionData(state.batch_index)`  
  `duckdb/src/function/table/arrow.cpp`
- **Table scan (seq_scan)**: returns batch index from the scan state  
  `duckdb/src/function/table/table_scan.cpp`
- **Multi-file scans** (e.g., CSV/Parquet via MultiFileFunction):  
  `MultiFileGetPartitionData` returns `OperatorPartitionData(data.batch_index)` and lets the reader add partition data.  
  `duckdb/src/include/duckdb/common/multi_file/multi_file_function.hpp`

## Semantics expected by DuckDB
1) **Monotonic per local sink**: for a given pipeline thread, `batch_index` must not decrease.  
2) **Globally meaningful**: indexes should reflect the logical source order for insertion-order preservation.  
3) **Stable**: batches with the same logical order should receive consistent indexes across runs.

If these are not respected, `PipelineExecutor::NextBatch` can fail with “invalid batch index” or “lower batch index” errors.

## What it would mean for H5DB (h5_read)

### Potential advantages
- **Enable parallel ordered COPY** when `preserve_insertion_order` is required and the sink supports batch copy.  
  Without batch indexes, DuckDB cannot select `BATCH_COPY_TO_FILE` and may fall back to single-threaded or non-batched copy.
- **Enable parallel ordered INSERT/CTAS** when insertion order must be preserved.  
  With batch indexes, the planner can select `PhysicalBatchInsert` instead of `PhysicalInsert`.
- **Preserve HDF5 row order at sinks** that reorder by batch index, even when scan completion is out of order.

### Potential disadvantages / risks
- **Completion is out-of-order** (by design), but assignment is ordered.  
  Batch indexes must reflect the logical row order, not the completion order.
- **Predicate pushdown** produces disjoint row ranges; batch indexes must still align with the logical row order, not the read order.

### What would be required to implement it safely
1) **Define the logical batch index**:  
   - The most direct mapping is `batch_index = range_start_row` (or `range_start_row / STANDARD_VECTOR_SIZE` if DuckDB expects dense batch numbers).  
   - `range_start_row` is already computed in `GetNextDataRange` and is monotonic in assignment order.
2) **Plumb range metadata into `get_partition_data`**:  
   - Today `h5_read` has no local state. To return the correct batch index, we would need to store the last assigned `position` per thread (e.g., via a local state or thread-local scan state).
3) **Handle filtered ranges**:  
   - Use the actual `range_start_row` from `valid_row_ranges` so gaps in filtered data preserve logical ordering.
4) **Decide on parallelism vs ordering**:  
   - Since assignment order is monotonic, parallel scans can still preserve order **if** the sink reorders by `batch_index`.  
   - If DuckDB expects dense indices, use a per-range counter instead of raw row positions.

### Minimal implementation option (risky)
Return `OperatorPartitionData(position / STANDARD_VECTOR_SIZE)` where `position` is the current scan start.  
This is only safe if **each local scan thread** sees a monotonic `position` sequence.

### Safer implementation option
Track a **per-thread batch counter** that advances only when the thread consumes a logically later range. This may still fail if ranges are claimed out of order across threads. To guarantee correctness, H5DB would need to:
- Partition the scan into **ordered, non-overlapping ranges** per thread, or
- Restrict to **single-threaded** execution when `partition_info` requests a batch index.

## Recommended next steps
1) Decide whether H5DB should ever attempt order preservation under parallel execution.
2) If yes, add a local state that records the assigned `range_start_row` and return it in `get_partition_data`.
3) If no, explicitly disable partition data and rely on single-threaded ordered mode when order matters.
