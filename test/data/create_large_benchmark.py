#!/usr/bin/env python3
"""Create a large HDF5 file for demonstrating parallel execution in htop."""
import h5py
import numpy as np

# Create a dataset large enough for 10-15 second queries
NUM_ROWS = 10_000_000  # 10 million rows

print(f"Creating large benchmark dataset with {NUM_ROWS:,} rows...")
print("This may take a minute...")

with h5py.File('large_benchmark.h5', 'w') as f:
    np.random.seed(42)

    # Integer column
    print("  Creating /integers...")
    integers = np.arange(NUM_ROWS, dtype=np.int64)
    f.create_dataset('/integers', data=integers)
    print(f"    ✓ {NUM_ROWS:,} int64 values")

    # Float column with random data
    print("  Creating /floats...")
    floats = np.random.randn(NUM_ROWS).astype(np.float64)
    f.create_dataset('/floats', data=floats)
    print(f"    ✓ {NUM_ROWS:,} float64 values")

    # Category column (for grouping)
    print("  Creating /categories...")
    categories = np.random.randint(0, 1000, size=NUM_ROWS, dtype=np.int32)
    f.create_dataset('/categories', data=categories)
    print(f"    ✓ {NUM_ROWS:,} int32 values (0-999)")

    # Values to aggregate
    print("  Creating /values...")
    values = np.random.randn(NUM_ROWS).astype(np.float64) * 100
    f.create_dataset('/values', data=values)
    print(f"    ✓ {NUM_ROWS:,} float64 values")

print(f"\nDataset size: ~{NUM_ROWS * 32 / 1024 / 1024:.1f} MB")
print("Done! File: large_benchmark.h5")
