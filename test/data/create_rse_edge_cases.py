#!/usr/bin/env python3
"""Create HDF5 test file with RSE edge cases.

Tests various edge cases for Run-Start Encoding (RSE) to ensure
the scanner handles all scenarios correctly, including:
- Single row datasets
- Single-entry runs (length = 1)
- Runs aligned with chunk boundaries (2048, 4096, etc.)
- Runs spanning multiple chunks
- Constant values (CONSTANT_VECTOR optimization)
- Different data types
"""

import h5py
import numpy as np

CHUNK_SIZE = 2048  # DuckDB's STANDARD_VECTOR_SIZE

def create_rse_dataset(f, group_name, index_data, run_starts, values, dtype=np.int32):
    """Helper to create RSE dataset with index, run_starts, and values."""
    grp = f.create_group(group_name)
    grp.create_dataset('index', data=index_data)
    grp.create_dataset('run_starts', data=run_starts, dtype=np.uint64)
    grp.create_dataset('values', data=values, dtype=dtype)
    return grp

with h5py.File('rse_edge_cases.h5', 'w') as f:

    # ==========================================================================
    # Test 1: Single row dataset
    # ==========================================================================
    create_rse_dataset(
        f, 'single_row',
        index_data=np.array([0], dtype=np.int32),
        run_starts=np.array([0], dtype=np.uint64),
        values=np.array([42], dtype=np.int32)
    )

    # ==========================================================================
    # Test 2: Single-entry runs (worst case for RLE, every row is different run)
    # ==========================================================================
    create_rse_dataset(
        f, 'single_entry_runs',
        index_data=np.arange(10, dtype=np.int32),
        run_starts=np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype=np.uint64),
        values=np.array([100, 200, 300, 400, 500, 600, 700, 800, 900, 1000], dtype=np.int32)
    )

    # ==========================================================================
    # Test 3: Runs aligned exactly with chunk boundary (2048)
    # First run: rows 0-2047 (2048 rows, exactly one chunk)
    # Second run: rows 2048-4095 (2048 rows, exactly one chunk)
    # Third run: rows 4096-4099 (4 rows, partial chunk)
    # ==========================================================================
    num_rows_aligned = 4100
    create_rse_dataset(
        f, 'chunk_aligned',
        index_data=np.arange(num_rows_aligned, dtype=np.int32),
        run_starts=np.array([0, 2048, 4096], dtype=np.uint64),
        values=np.array([1, 2, 3], dtype=np.int32)
    )

    # ==========================================================================
    # Test 4: Single run spanning multiple chunks (constant value)
    # 5000 rows all with same value - tests CONSTANT_VECTOR optimization
    # ==========================================================================
    num_rows_constant = 5000
    create_rse_dataset(
        f, 'constant_multi_chunk',
        index_data=np.arange(num_rows_constant, dtype=np.int32),
        run_starts=np.array([0], dtype=np.uint64),
        values=np.array([999], dtype=np.int32)
    )

    # ==========================================================================
    # Test 5: Run boundary in middle of chunk
    # First run: 0-1000 (1001 rows)
    # Second run: 1001-3000 (1999 rows, spans chunk boundary at 2048)
    # Third run: 3001-3499 (499 rows)
    # ==========================================================================
    create_rse_dataset(
        f, 'mid_chunk_boundary',
        index_data=np.arange(3500, dtype=np.int32),
        run_starts=np.array([0, 1001, 3001], dtype=np.uint64),
        values=np.array([10, 20, 30], dtype=np.int32)
    )

    # ==========================================================================
    # Test 6: Large run followed by many small runs
    # First run: 0-3000 (3001 rows, spans chunks 0, 1, partial 2)
    # Then: 10 runs of 1 row each (3001-3010)
    # ==========================================================================
    create_rse_dataset(
        f, 'large_then_small',
        index_data=np.arange(3011, dtype=np.int32),
        run_starts=np.array([0, 3001, 3002, 3003, 3004, 3005, 3006, 3007, 3008, 3009, 3010], dtype=np.uint64),
        values=np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], dtype=np.int32)
    )

    # ==========================================================================
    # Test 7: Different data types - int8
    # ==========================================================================
    create_rse_dataset(
        f, 'type_int8',
        index_data=np.arange(100, dtype=np.int32),
        run_starts=np.array([0, 50], dtype=np.uint64),
        values=np.array([127, -128], dtype=np.int8),
        dtype=np.int8
    )

    # ==========================================================================
    # Test 8: Different data types - int64
    # ==========================================================================
    create_rse_dataset(
        f, 'type_int64',
        index_data=np.arange(100, dtype=np.int32),
        run_starts=np.array([0, 25, 75], dtype=np.uint64),
        values=np.array([9223372036854775807, -9223372036854775808, 0], dtype=np.int64),
        dtype=np.int64
    )

    # ==========================================================================
    # Test 9: Different data types - float32
    # ==========================================================================
    create_rse_dataset(
        f, 'type_float32',
        index_data=np.arange(100, dtype=np.int32),
        run_starts=np.array([0, 33, 67], dtype=np.uint64),
        values=np.array([3.14159, 2.71828, 1.41421], dtype=np.float32),
        dtype=np.float32
    )

    # ==========================================================================
    # Test 10: Different data types - float64
    # ==========================================================================
    create_rse_dataset(
        f, 'type_float64',
        index_data=np.arange(100, dtype=np.int32),
        run_starts=np.array([0, 40, 80], dtype=np.uint64),
        values=np.array([3.141592653589793, 2.718281828459045, 1.4142135623730951], dtype=np.float64),
        dtype=np.float64
    )

    # ==========================================================================
    # Test 11: Different data types - string
    # ==========================================================================
    grp = f.create_group('type_string')
    grp.create_dataset('index', data=np.arange(50, dtype=np.int32))
    grp.create_dataset('run_starts', data=np.array([0, 20, 40], dtype=np.uint64))
    grp.create_dataset('values', data=np.array(['alpha', 'beta', 'gamma'], dtype=h5py.string_dtype()))

    # ==========================================================================
    # Test 12: Exact chunk size (2048 rows, single run)
    # Tests CONSTANT_VECTOR optimization for exactly one chunk
    # ==========================================================================
    create_rse_dataset(
        f, 'exact_one_chunk',
        index_data=np.arange(CHUNK_SIZE, dtype=np.int32),
        run_starts=np.array([0], dtype=np.uint64),
        values=np.array([777], dtype=np.int32)
    )

    # ==========================================================================
    # Test 13: Exactly 2 chunks (4096 rows, single run)
    # ==========================================================================
    create_rse_dataset(
        f, 'exact_two_chunks',
        index_data=np.arange(CHUNK_SIZE * 2, dtype=np.int32),
        run_starts=np.array([0], dtype=np.uint64),
        values=np.array([888], dtype=np.int32)
    )

    # ==========================================================================
    # Test 14: Chunk + 1 row (2049 rows, single run)
    # First chunk is constant, second chunk has 1 row
    # ==========================================================================
    create_rse_dataset(
        f, 'chunk_plus_one',
        index_data=np.arange(CHUNK_SIZE + 1, dtype=np.int32),
        run_starts=np.array([0], dtype=np.uint64),
        values=np.array([111], dtype=np.int32)
    )

    # ==========================================================================
    # Test 15: Chunk - 1 row (2047 rows, single run)
    # Just under one chunk
    # ==========================================================================
    create_rse_dataset(
        f, 'chunk_minus_one',
        index_data=np.arange(CHUNK_SIZE - 1, dtype=np.int32),
        run_starts=np.array([0], dtype=np.uint64),
        values=np.array([222], dtype=np.int32)
    )

    # ==========================================================================
    # Test 16: Multiple runs within first chunk
    # 5 runs of ~400 rows each within first chunk (total 2000 < 2048)
    # ==========================================================================
    create_rse_dataset(
        f, 'multi_runs_one_chunk',
        index_data=np.arange(2000, dtype=np.int32),
        run_starts=np.array([0, 400, 800, 1200, 1600], dtype=np.uint64),
        values=np.array([1, 2, 3, 4, 5], dtype=np.int32)
    )

    # ==========================================================================
    # Test 17: Alternating pattern crossing chunk boundary
    # Pattern: 1000 rows of A, 1000 of B, 1000 of A, 1000 of B
    # Boundaries at 1000, 2000 (crosses chunk at 2048), 3000
    # ==========================================================================
    create_rse_dataset(
        f, 'alternating_cross_chunk',
        index_data=np.arange(4000, dtype=np.int32),
        run_starts=np.array([0, 1000, 2000, 3000], dtype=np.uint64),
        values=np.array([100, 200, 100, 200], dtype=np.int32)
    )

    # ==========================================================================
    # Test 18: Very small dataset (3 rows, 2 runs)
    # ==========================================================================
    create_rse_dataset(
        f, 'very_small',
        index_data=np.array([0, 1, 2], dtype=np.int32),
        run_starts=np.array([0, 2], dtype=np.uint64),
        values=np.array([10, 20], dtype=np.int32)
    )

print("Created rse_edge_cases.h5 successfully!")
print(f"Chunk size used: {CHUNK_SIZE}")
