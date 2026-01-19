#!/usr/bin/env python3
"""Create large-scale HDF5 test file for parallelism testing.

This creates a large file with ~10M rows to trigger DuckDB's parallelism
(which activates at >128K rows per thread). With 10M rows and 8MB chunks,
we should see multiple chunk fetches and parallel execution.

Target: 10M rows = ~10 chunks of 8MB each (1M doubles per chunk)
"""

import h5py
import numpy as np

# Target 10M rows to ensure multiple chunks and parallel execution
NUM_ROWS = 10_000_000

print(f"Creating large-scale test file with {NUM_ROWS:,} rows...")
print(f"Expected file size: ~{NUM_ROWS * 8 / 1024 / 1024:.0f} MB for doubles")

with h5py.File('large_simple.h5', 'w') as f:
    # 1D Numeric datasets
    print("\nCreating 1D numeric datasets...")

    # Integers: 0, 1, 2, ..., NUM_ROWS-1
    # SUM = NUM_ROWS * (NUM_ROWS - 1) / 2
    integers = np.arange(NUM_ROWS, dtype=np.int32)
    f.create_dataset('integers', data=integers, chunks=(1_000_000,))
    print(f"  /integers: {len(integers):,} int32 values, SUM={integers.sum():,}")

    # Floats: random values in [0, 1)
    np.random.seed(42)
    floats = np.random.random(NUM_ROWS).astype(np.float64)
    f.create_dataset('floats', data=floats, chunks=(1_000_000,))
    print(f"  /floats: {len(floats):,} float64 values, MEAN={floats.mean():.6f}")

    # 2D Array (matrix): shape (NUM_ROWS, 4)
    print("\nCreating 2D array dataset...")
    # Each row: [i*4, i*4+1, i*4+2, i*4+3]
    matrix = np.arange(NUM_ROWS * 4, dtype=np.int32).reshape(NUM_ROWS, 4)
    f.create_dataset('matrix', data=matrix, chunks=(250_000, 4))
    print(f"  /matrix: shape {matrix.shape}, total SUM={matrix.sum():,}")

    # Different integer types
    print("\nCreating various integer type datasets...")

    # int8: cycling through -128 to 127
    int8_data = np.arange(NUM_ROWS, dtype=np.int64) % 256 - 128
    int8_data = int8_data.astype(np.int8)
    f.create_dataset('int8', data=int8_data, chunks=(1_000_000,))
    print(f"  /int8: {len(int8_data):,} int8 values")

    # int16
    int16_data = np.arange(NUM_ROWS, dtype=np.int16) % 10000
    f.create_dataset('int16', data=int16_data, chunks=(1_000_000,))
    print(f"  /int16: {len(int16_data):,} int16 values")

    # int64
    int64_data = np.arange(NUM_ROWS, dtype=np.int64)
    f.create_dataset('int64', data=int64_data, chunks=(1_000_000,))
    print(f"  /int64: {len(int64_data):,} int64 values")

    # Unsigned integers
    print("\nCreating unsigned integer type datasets...")

    uint8_data = np.arange(NUM_ROWS, dtype=np.uint64) % 256
    uint8_data = uint8_data.astype(np.uint8)
    f.create_dataset('uint8', data=uint8_data, chunks=(1_000_000,))
    print(f"  /uint8: {len(uint8_data):,} uint8 values, MAX={uint8_data.max()}")

    uint16_data = np.arange(NUM_ROWS, dtype=np.uint16) % 10000
    f.create_dataset('uint16', data=uint16_data, chunks=(1_000_000,))
    print(f"  /uint16: {len(uint16_data):,} uint16 values")

    uint32_data = np.arange(NUM_ROWS, dtype=np.uint32)
    f.create_dataset('uint32', data=uint32_data, chunks=(1_000_000,))
    print(f"  /uint32: {len(uint32_data):,} uint32 values")

    # Float types
    print("\nCreating float type datasets...")

    np.random.seed(43)
    float32_data = np.random.random(NUM_ROWS).astype(np.float32)
    f.create_dataset('float32', data=float32_data, chunks=(1_000_000,))
    print(f"  /float32: {len(float32_data):,} float32 values")

    float64_data = np.random.random(NUM_ROWS).astype(np.float64)
    f.create_dataset('float64', data=float64_data, chunks=(1_000_000,))
    print(f"  /float64: {len(float64_data):,} float64 values")

    # Nested groups
    print("\nCreating nested group structure...")

    group1 = f.create_group('group1')

    # group1/data1: float values
    data1 = np.random.random(NUM_ROWS).astype(np.float64)
    group1.create_dataset('data1', data=data1, chunks=(1_000_000,))
    print(f"  /group1/data1: {len(data1):,} float64 values")

    # group1/data2: integer values
    data2 = np.arange(NUM_ROWS, dtype=np.int32)
    group1.create_dataset('data2', data=data2, chunks=(1_000_000,))
    print(f"  /group1/data2: {len(data2):,} int32 values")

    # Nested subgroup
    subgroup = group1.create_group('subgroup')
    nested_data = np.arange(NUM_ROWS, dtype=np.int32)
    subgroup.create_dataset('nested_data', data=nested_data, chunks=(1_000_000,))
    print(f"  /group1/subgroup/nested_data: {len(nested_data):,} int32 values")

    # Multi-dimensional arrays
    print("\nCreating multi-dimensional arrays...")

    # 1D array
    array_1d = np.arange(NUM_ROWS, dtype=np.int64)
    f.create_dataset('array_1d', data=array_1d, chunks=(1_000_000,))
    print(f"  /array_1d: shape {array_1d.shape}")

    # 2D array: (NUM_ROWS, 4)
    array_2d = np.arange(NUM_ROWS * 4, dtype=np.int64).reshape(NUM_ROWS, 4)
    f.create_dataset('array_2d', data=array_2d, chunks=(250_000, 4))
    print(f"  /array_2d: shape {array_2d.shape}")

    # 3D array: (NUM_ROWS, 3, 4) - note: smaller for memory
    # Only create subset to avoid huge file
    array_3d_rows = min(NUM_ROWS, 1_000_000)  # Limit to 1M for 3D
    array_3d = np.arange(array_3d_rows * 3 * 4, dtype=np.int64).reshape(array_3d_rows, 3, 4)
    f.create_dataset('array_3d', data=array_3d, chunks=(100_000, 3, 4))
    print(f"  /array_3d: shape {array_3d.shape}")

    # 4D array: (NUM_ROWS, 2, 3, 4) - smaller subset
    array_4d_rows = min(NUM_ROWS, 500_000)  # Limit to 500K for 4D
    array_4d = np.arange(array_4d_rows * 2 * 3 * 4, dtype=np.int64).reshape(array_4d_rows, 2, 3, 4)
    f.create_dataset('array_4d', data=array_4d, chunks=(50_000, 2, 3, 4))
    print(f"  /array_4d: shape {array_4d.shape}")

    # Add metadata
    f.attrs['description'] = 'Large-scale test file for parallel execution testing'
    f.attrs['num_rows_1d'] = NUM_ROWS
    f.attrs['created_by'] = 'create_large_simple.py'
    f.attrs['purpose'] = 'Trigger DuckDB parallelism (>128K rows/thread) and multiple chunk fetches (8MB/chunk)'

print(f"\nOK File created successfully!")
print(f"Filename: large_simple.h5")
