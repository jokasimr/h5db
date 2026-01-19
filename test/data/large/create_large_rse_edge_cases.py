#!/usr/bin/env python3
"""Create large-scale RSE edge cases test file.

This creates large-scale versions of all RSE edge case scenarios to test
parallel execution with challenging run patterns.

Test scenarios:
1. Single row dataset (unchanged - 1 row is the edge case)
2. Single-entry runs: 1M rows, each row is a different run (worst case for RLE)
3. Chunk-aligned runs: Runs exactly aligned with 2048-row (STANDARD_VECTOR_SIZE) chunks
4. Large single run: 10M rows all with same value (CONSTANT_VECTOR optimization)
5. Alternating runs: Alternating between 2 values every 2048 rows
6. Mid-chunk boundaries: Runs that split chunks in the middle
7. Many small runs: Frequent value changes (avg 100 rows/run over 10M rows)
8. Different data types at scale
"""

import h5py
import numpy as np

print("Creating large-scale RSE edge cases test file...")

with h5py.File('large_rse_edge_cases.h5', 'w') as f:

    # ==========================================================================
    # Test 1: Single Row Dataset (unchanged from small version)
    # ==========================================================================
    print("\n1. Creating single row dataset...")
    grp = f.create_group('single_row')
    grp.create_dataset('index', data=np.array([0], dtype=np.int32))
    grp.create_dataset('run_starts', data=np.array([0], dtype=np.uint64))
    grp.create_dataset('values', data=np.array([42], dtype=np.int32))
    print("  /single_row: 1 row")

    # ==========================================================================
    # Test 2: Single-Entry Runs (1M rows, worst case for RLE)
    # ==========================================================================
    print("\n2. Creating single-entry runs (1M rows)...")
    NUM_SINGLE_ENTRY = 1_000_000
    grp = f.create_group('single_entry_runs')

    # Index column
    index = np.arange(NUM_SINGLE_ENTRY, dtype=np.int32)
    grp.create_dataset('index', data=index, chunks=(100_000,))

    # Every row is a different run - values cycle through 1000-1999
    run_starts = np.arange(NUM_SINGLE_ENTRY, dtype=np.uint64)
    values = (np.arange(NUM_SINGLE_ENTRY, dtype=np.int32) % 1000) + 1000

    grp.create_dataset('run_starts', data=run_starts, chunks=(100_000,))
    grp.create_dataset('values', data=values, chunks=(100_000,))
    print(f"  /single_entry_runs: {NUM_SINGLE_ENTRY:,} rows, {NUM_SINGLE_ENTRY:,} runs")
    print(f"  Values range: {values.min()}-{values.max()}")

    # ==========================================================================
    # Test 3: Chunk-Aligned Runs (2048-row aligned chunks)
    # ==========================================================================
    print("\n3. Creating chunk-aligned runs...")
    CHUNK_SIZE = 2048
    NUM_CHUNKS = 5000  # 5000 chunks = 10.24M rows
    NUM_CHUNK_ALIGNED = NUM_CHUNKS * CHUNK_SIZE

    grp = f.create_group('chunk_aligned')

    # Index column
    index = np.arange(NUM_CHUNK_ALIGNED, dtype=np.int32)
    grp.create_dataset('index', data=index, chunks=(CHUNK_SIZE * 100,))

    # Each chunk is a single run, cycling through values 1-10
    run_starts = np.arange(0, NUM_CHUNK_ALIGNED, CHUNK_SIZE, dtype=np.uint64)
    values = (np.arange(NUM_CHUNKS, dtype=np.int32) % 10) + 1

    grp.create_dataset('run_starts', data=run_starts, chunks=(min(10_000, NUM_CHUNKS),))
    grp.create_dataset('values', data=values, chunks=(min(10_000, NUM_CHUNKS),))
    print(f"  /chunk_aligned: {NUM_CHUNK_ALIGNED:,} rows, {NUM_CHUNKS:,} runs")
    print(f"  Each run is exactly {CHUNK_SIZE} rows (STANDARD_VECTOR_SIZE)")

    # ==========================================================================
    # Test 4: Large Single Run (10M rows, all same value)
    # ==========================================================================
    print("\n4. Creating large single run (10M rows)...")
    NUM_SINGLE_RUN = 10_000_000

    grp = f.create_group('large_single_run')

    # Index column
    index = np.arange(NUM_SINGLE_RUN, dtype=np.int32)
    grp.create_dataset('index', data=index, chunks=(1_000_000,))

    # All 10M rows have value 777
    run_starts = np.array([0], dtype=np.uint64)
    values = np.array([777], dtype=np.int32)

    grp.create_dataset('run_starts', data=run_starts)
    grp.create_dataset('values', data=values)
    print(f"  /large_single_run: {NUM_SINGLE_RUN:,} rows, 1 run (all value=777)")
    print(f"  Tests CONSTANT_VECTOR optimization across all chunks")

    # ==========================================================================
    # Test 5: Alternating Runs (every 2048 rows)
    # ==========================================================================
    print("\n5. Creating alternating runs...")
    NUM_ALTERNATING = 10_000_000
    RUN_LENGTH = 2048

    grp = f.create_group('alternating_runs')

    # Index column
    index = np.arange(NUM_ALTERNATING, dtype=np.int32)
    grp.create_dataset('index', data=index, chunks=(1_000_000,))

    # Alternating between values 100 and 200 every 2048 rows
    run_starts = np.arange(0, NUM_ALTERNATING, RUN_LENGTH, dtype=np.uint64)
    num_runs = len(run_starts)  # Use actual length to avoid mismatch
    values = np.array([100 if i % 2 == 0 else 200 for i in range(num_runs)], dtype=np.int32)

    grp.create_dataset('run_starts', data=run_starts, chunks=(min(10_000, len(run_starts)),))
    grp.create_dataset('values', data=values, chunks=(min(10_000, len(values)),))
    print(f"  /alternating_runs: {NUM_ALTERNATING:,} rows, {num_runs:,} runs")
    print(f"  Alternates between 100 and 200 every {RUN_LENGTH} rows")

    # ==========================================================================
    # Test 6: Mid-Chunk Boundaries
    # ==========================================================================
    print("\n6. Creating mid-chunk boundaries...")
    NUM_MID_CHUNK = 10_000_000
    RUN_LENGTH = 1500  # Not aligned with 2048

    grp = f.create_group('mid_chunk_boundaries')

    # Index column
    index = np.arange(NUM_MID_CHUNK, dtype=np.int32)
    grp.create_dataset('index', data=index, chunks=(1_000_000,))

    # Runs of 1500 rows each, cycling through values 10-19
    num_runs = NUM_MID_CHUNK // RUN_LENGTH + 1
    run_starts = np.arange(0, NUM_MID_CHUNK, RUN_LENGTH, dtype=np.uint64)
    values = (np.arange(num_runs, dtype=np.int32) % 10) + 10

    grp.create_dataset('run_starts', data=run_starts, chunks=(min(10_000, len(run_starts)),))
    grp.create_dataset('values', data=values[: len(run_starts)], chunks=(min(10_000, len(run_starts)),))
    print(f"  /mid_chunk_boundaries: {NUM_MID_CHUNK:,} rows, {len(run_starts):,} runs")
    print(f"  Each run is {RUN_LENGTH} rows (not aligned with 2048)")

    # ==========================================================================
    # Test 7: Many Small Runs (avg 100 rows/run over 10M rows)
    # ==========================================================================
    print("\n7. Creating many small runs...")
    NUM_SMALL_RUNS = 10_000_000
    AVG_RUN_LENGTH = 100

    grp = f.create_group('many_small_runs')

    # Index column
    index = np.arange(NUM_SMALL_RUNS, dtype=np.int32)
    grp.create_dataset('index', data=index, chunks=(1_000_000,))

    # Varying run lengths around 100 (50-150 rows per run)
    np.random.seed(42)
    run_lengths = np.random.randint(50, 150, NUM_SMALL_RUNS // AVG_RUN_LENGTH)
    run_starts = np.concatenate([[0], np.cumsum(run_lengths)[:-1]]).astype(np.uint64)

    # Trim to fit exactly NUM_SMALL_RUNS
    run_starts = run_starts[run_starts < NUM_SMALL_RUNS]
    values = (np.arange(len(run_starts), dtype=np.int32) % 500) + 500

    grp.create_dataset('run_starts', data=run_starts, chunks=(min(10_000, len(run_starts)),))
    grp.create_dataset('values', data=values, chunks=(min(10_000, len(values)),))
    print(f"  /many_small_runs: {NUM_SMALL_RUNS:,} rows, {len(run_starts):,} runs")
    print(f"  Average run length: ~{AVG_RUN_LENGTH} rows")

    # ==========================================================================
    # Test 8: Different Data Types at Scale
    # ==========================================================================
    print("\n8. Creating different data types...")
    NUM_TYPES = 5_000_000
    RUN_LENGTH = 10000  # 500 runs

    # Float64 RSE
    grp_float = f.create_group('large_float_rse')
    index = np.arange(NUM_TYPES, dtype=np.int32)
    grp_float.create_dataset('index', data=index, chunks=(500_000,))

    num_runs = NUM_TYPES // RUN_LENGTH
    run_starts = np.arange(0, NUM_TYPES, RUN_LENGTH, dtype=np.uint64)
    values = np.linspace(1.0, 100.0, num_runs, dtype=np.float64)

    grp_float.create_dataset('run_starts', data=run_starts, chunks=(min(10_000, len(run_starts)),))
    grp_float.create_dataset('values', data=values, chunks=(min(10_000, len(values)),))
    print(f"  /large_float_rse: {NUM_TYPES:,} rows, {num_runs} runs (float64)")

    # String RSE
    grp_string = f.create_group('large_string_rse')
    index = np.arange(NUM_TYPES, dtype=np.int32)
    grp_string.create_dataset('index', data=index, chunks=(500_000,))

    # Cycle through "value_000" to "value_499"
    string_values = np.array([f"value_{i:03d}" for i in range(num_runs)], dtype='S20')

    grp_string.create_dataset('run_starts', data=run_starts, chunks=(min(10_000, len(run_starts)),))
    grp_string.create_dataset('values', data=string_values, chunks=(min(10_000, len(string_values)),))
    print(f"  /large_string_rse: {NUM_TYPES:,} rows, {num_runs} runs (string)")

    # Int64 RSE
    grp_int64 = f.create_group('large_int64_rse')
    index = np.arange(NUM_TYPES, dtype=np.int32)
    grp_int64.create_dataset('index', data=index, chunks=(500_000,))

    values_int64 = np.arange(num_runs, dtype=np.int64) * 1000000

    grp_int64.create_dataset('run_starts', data=run_starts, chunks=(min(10_000, len(run_starts)),))
    grp_int64.create_dataset('values', data=values_int64, chunks=(min(10_000, len(values_int64)),))
    print(f"  /large_int64_rse: {NUM_TYPES:,} rows, {num_runs} runs (int64)")

    # Add metadata
    f.attrs['description'] = 'Large-scale RSE edge cases test file'
    f.attrs['created_by'] = 'create_large_rse_edge_cases.py'
    f.attrs['purpose'] = 'Test RSE edge cases with parallel execution'
    f.attrs['scenarios'] = (
        'single_row, single_entry_runs, chunk_aligned, large_single_run, alternating_runs, mid_chunk_boundaries, many_small_runs, type_variants'
    )

print(f"\n✓ File created successfully!")
print(f"Filename: large_rse_edge_cases.h5")
print(f"\nTotal test scenarios: 11 groups (1 + 2 + 3 + 3 + 2)")
