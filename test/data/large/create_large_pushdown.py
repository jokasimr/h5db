#!/usr/bin/env python3
"""Create large-scale predicate pushdown test file.

This creates a 10M row version of pushdown_test.h5 for parallel execution testing.
The original has 1000 rows (5 runs × 200 rows), this scales to 10M rows (5 runs × 2M rows).

Original structure (1000 rows):
- Sorted int32 RSE: values [10, 20, 30, 40, 50], 200 rows per run
- Unsorted int32 RSE: values [50, 10, 30, 20, 40], 200 rows per run
- Sorted float RSE: values [1.5, 2.5, 3.5, 4.5, 5.5], 200 rows per run
- Sorted int64 RSE: values [100, 200, 300, 400, 500], 200 rows per run
- Sorted uint32 RSE: values [1000, 2000, 3000, 4000, 5000], 200 rows per run
- Regular int32: values 0-999

Large structure (10M rows):
- Same RSE patterns but with 2M rows per run (10M total)
- Regular int32: values 0-9999999
"""

import h5py
import numpy as np

NUM_ROWS = 10_000_000
ROWS_PER_RUN = NUM_ROWS // 5  # 2,000,000 rows per run

print(f"Creating large-scale predicate pushdown test file with {NUM_ROWS:,} rows...")
print(f"Rows per run: {ROWS_PER_RUN:,}")

with h5py.File('large_pushdown_test.h5', 'w') as f:
    # Regular column: 0, 1, 2, ..., NUM_ROWS-1
    print("\nCreating regular column...")
    regular = np.arange(NUM_ROWS, dtype=np.int32)
    f.create_dataset('regular', data=regular, chunks=(1_000_000,))
    print(f"  /regular: {len(regular):,} int32 values")

    # Regular float column: 0.0, 0.1, 0.2, ..., (NUM_ROWS-1)*0.1
    print("\nCreating regular float column...")
    regular_float = np.arange(NUM_ROWS, dtype=np.float64) * 0.1
    f.create_dataset('regular_float', data=regular_float, chunks=(1_000_000,))
    print(f"  /regular_float: {len(regular_float):,} float64 values")

    # Sorted int32 RSE: [10, 20, 30, 40, 50]
    print("\nCreating sorted int32 RSE columns...")
    int_rse_values = np.array([10, 20, 30, 40, 50], dtype=np.int32)
    int_rse_starts = np.array([0, ROWS_PER_RUN, 2 * ROWS_PER_RUN, 3 * ROWS_PER_RUN, 4 * ROWS_PER_RUN], dtype=np.uint64)

    f.create_dataset('int_rse_starts', data=int_rse_starts)
    f.create_dataset('int_rse_values', data=int_rse_values)
    print(f"  /int_rse_starts: {len(int_rse_starts)} runs")
    print(f"  /int_rse_values: {int_rse_values}")

    # Unsorted int32 RSE: [50, 10, 30, 20, 40]
    print("\nCreating unsorted int32 RSE columns...")
    int_unsorted_rse_values = np.array([50, 10, 30, 20, 40], dtype=np.int32)
    int_unsorted_rse_starts = int_rse_starts.copy()

    f.create_dataset('int_unsorted_rse_starts', data=int_unsorted_rse_starts)
    f.create_dataset('int_unsorted_rse_values', data=int_unsorted_rse_values)
    print(f"  /int_unsorted_rse_starts: {len(int_unsorted_rse_starts)} runs")
    print(f"  /int_unsorted_rse_values: {int_unsorted_rse_values}")

    # Sorted float RSE: [1.5, 2.5, 3.5, 4.5, 5.5]
    print("\nCreating sorted float RSE columns...")
    float_rse_values = np.array([1.5, 2.5, 3.5, 4.5, 5.5], dtype=np.float64)
    float_rse_starts = int_rse_starts.copy()

    f.create_dataset('float_rse_starts', data=float_rse_starts)
    f.create_dataset('float_rse_values', data=float_rse_values)
    print(f"  /float_rse_starts: {len(float_rse_starts)} runs")
    print(f"  /float_rse_values: {float_rse_values}")

    # Sorted int64 RSE: [100, 200, 300, 400, 500]
    print("\nCreating sorted int64 RSE columns...")
    int64_rse_values = np.array([100, 200, 300, 400, 500], dtype=np.int64)
    int64_rse_starts = int_rse_starts.copy()

    f.create_dataset('int64_rse_starts', data=int64_rse_starts)
    f.create_dataset('int64_rse_values', data=int64_rse_values)
    print(f"  /int64_rse_starts: {len(int64_rse_starts)} runs")
    print(f"  /int64_rse_values: {int64_rse_values}")

    # Sorted uint32 RSE: [1000, 2000, 3000, 4000, 5000]
    print("\nCreating sorted uint32 RSE columns...")
    uint32_rse_values = np.array([1000, 2000, 3000, 4000, 5000], dtype=np.uint32)
    uint32_rse_starts = int_rse_starts.copy()

    f.create_dataset('uint32_rse_starts', data=uint32_rse_starts)
    f.create_dataset('uint32_rse_values', data=uint32_rse_values)
    print(f"  /uint32_rse_starts: {len(uint32_rse_starts)} runs")
    print(f"  /uint32_rse_values: {uint32_rse_values}")

    # String RSE: ["alpha", "beta", "gamma", "delta", "epsilon"]
    print("\nCreating string RSE columns...")
    string_rse_values = np.array(["alpha", "beta", "gamma", "delta", "epsilon"], dtype='S10')
    string_rse_starts = int_rse_starts.copy()

    f.create_dataset('string_rse_starts', data=string_rse_starts)
    f.create_dataset('string_rse_values', data=string_rse_values)
    print(f"  /string_rse_starts: {len(string_rse_starts)} runs")
    print(f"  /string_rse_values: {[s.decode() for s in string_rse_values]}")

    # Add metadata
    f.attrs['description'] = 'Large-scale predicate pushdown test file'
    f.attrs['num_rows'] = NUM_ROWS
    f.attrs['rows_per_run'] = ROWS_PER_RUN
    f.attrs['num_runs'] = 5
    f.attrs['created_by'] = 'create_large_pushdown.py'
    f.attrs['purpose'] = 'Test RSE predicate pushdown with parallel execution'

print(f"\n✓ File created successfully!")
print(f"Filename: large_pushdown_test.h5")
print(f"\nExpected query behaviors:")
print(f"  - SELECT WHERE int_rse_values = 30: Should scan {ROWS_PER_RUN:,} rows (1/5 of total)")
print(f"  - SELECT WHERE int_rse_values BETWEEN 20 AND 40: Should scan {3*ROWS_PER_RUN:,} rows (3/5 of total)")
print(f"  - SELECT WHERE int_rse_values != 30: Should scan all {NUM_ROWS:,} rows (no optimization)")
