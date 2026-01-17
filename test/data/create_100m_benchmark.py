#!/usr/bin/env python3
"""Create 100M row dataset for 12+ second queries."""
import h5py
import numpy as np

NUM_ROWS = 100_000_000  # 100 million rows

print(f"Creating 100M row dataset...")
print("This will take 5-10 minutes...")

with h5py.File('benchmark_100m.h5', 'w') as f:
    chunk_size = 10_000_000

    for dataset_name, dtype in [('/integers', np.int64), ('/floats', np.float64), ('/values', np.float64)]:
        print(f"  Creating {dataset_name}...")
        ds = f.create_dataset(dataset_name, shape=(NUM_ROWS,), dtype=dtype)

        for i in range(0, NUM_ROWS, chunk_size):
            end = min(i + chunk_size, NUM_ROWS)
            size = end - i

            if dataset_name == '/integers':
                data = np.arange(i, end, dtype=dtype)
            else:
                data = np.random.randn(size).astype(dtype)
                if dataset_name == '/values':
                    data *= 100

            ds[i:end] = data
            pct = 100 * end // NUM_ROWS
            print(f"    [{pct:3d}%] {end:,} / {NUM_ROWS:,}")

print(f"\nDataset: ~{NUM_ROWS * 24 / 1024 / 1024:.0f} MB")
print("Done!")
