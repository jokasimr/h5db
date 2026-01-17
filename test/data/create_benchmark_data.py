#!/usr/bin/env python3
"""Create a large HDF5 file for parallel execution benchmarking."""
import h5py
import numpy as np

# Create a dataset large enough to clearly show parallelism
# We want enough data that processing time >> scan time
NUM_ROWS = 1_000_000

print(f"Creating benchmark dataset with {NUM_ROWS:,} rows...")

with h5py.File('benchmark.h5', 'w') as f:
    # Create multiple columns for realistic queries
    np.random.seed(42)

    # Integer column
    integers = np.arange(NUM_ROWS, dtype=np.int64)
    f.create_dataset('/integers', data=integers)
    print(f"  Created /integers: {NUM_ROWS:,} int64 values")

    # Float column with random data
    floats = np.random.randn(NUM_ROWS).astype(np.float64)
    f.create_dataset('/floats', data=floats)
    print(f"  Created /floats: {NUM_ROWS:,} float64 values")

    # Category column (for grouping)
    categories = np.random.randint(0, 100, size=NUM_ROWS, dtype=np.int32)
    f.create_dataset('/categories', data=categories)
    print(f"  Created /categories: {NUM_ROWS:,} int32 values (0-99)")

    # Values to aggregate
    values = np.random.randn(NUM_ROWS).astype(np.float64) * 100
    f.create_dataset('/values', data=values)
    print(f"  Created /values: {NUM_ROWS:,} float64 values")

print(f"\nDataset size: ~{NUM_ROWS * 32 / 1024 / 1024:.1f} MB")
print("Done!")
