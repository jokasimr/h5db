#!/usr/bin/env python3
"""Create a huge HDF5 file for long-running parallel demo."""
import h5py
import numpy as np

# 50 million rows = ~12-15 second queries on single thread
NUM_ROWS = 50_000_000

print(f"Creating HUGE benchmark dataset with {NUM_ROWS:,} rows...")
print("This will take 2-3 minutes...")

with h5py.File('huge_benchmark.h5', 'w') as f:
    np.random.seed(42)

    # Create in chunks to avoid memory issues
    chunk_size = 5_000_000

    for dataset_name, dtype, desc in [
        ('/integers', np.int64, 'integers'),
        ('/floats', np.float64, 'floats'),
        ('/categories', np.int32, 'categories'),
        ('/values', np.float64, 'values'),
    ]:
        print(f"  Creating {dataset_name}...")
        ds = f.create_dataset(dataset_name, shape=(NUM_ROWS,), dtype=dtype)

        for i in range(0, NUM_ROWS, chunk_size):
            end = min(i + chunk_size, NUM_ROWS)
            size = end - i

            if dataset_name == '/integers':
                data = np.arange(i, end, dtype=dtype)
            elif dataset_name == '/categories':
                data = np.random.randint(0, 1000, size=size, dtype=dtype)
            else:
                data = np.random.randn(size).astype(dtype)
                if dataset_name == '/values':
                    data *= 100

            ds[i:end] = data
            print(f"    {end:,} / {NUM_ROWS:,} rows ({100*end//NUM_ROWS}%)")

print(f"\nDataset size: ~{NUM_ROWS * 32 / 1024 / 1024:.1f} MB")
print("Done! File: huge_benchmark.h5")
