#!/usr/bin/env python3
"""
Create large_rse_test.h5 for RSE multithreading regression tests.

This file tests that RSE state is NOT shared across threads, which was a critical bug.
Structure:
- 1,000,000 rows
- 2 RSE columns with different run distributions
- Used to verify parallel scanning produces correct GROUP BY results
"""

import numpy as np
import h5py


def create_large_rse_test(filename='large_rse_test.h5'):
    """Create large RSE test file with 1M rows."""
    print(f"Creating {filename}...")

    # No need to create directory - script is run from the target directory

    n_rows = 1_000_000

    with h5py.File(filename, 'w') as f:
        # Regular datasets
        f.create_dataset('index', data=np.arange(n_rows, dtype=np.int64))
        # values cycles 0-99 (test expects MAX(values) = 99)
        f.create_dataset('values', data=np.arange(n_rows, dtype=np.int64) % 100)

        # RSE Column 1: status with 4 distinct values
        # Distribution: 100k of value 0, 300k of value 1, 200k of value 2, 400k of value 3
        status_run_starts = np.array([0, 100_000, 400_000, 600_000], dtype=np.int64)
        status_values = np.array([0, 1, 2, 3], dtype=np.int32)

        f.create_dataset('status_run_starts', data=status_run_starts)
        f.create_dataset('status_values', data=status_values)

        # RSE Column 2: category with 20 runs alternating between 2 values
        # Each run is 50k rows, alternating between value 10 and 20
        category_run_starts = np.arange(0, n_rows, 50_000, dtype=np.int64)
        category_values = np.array([10 if i % 2 == 0 else 20 for i in range(20)], dtype=np.int32)

        f.create_dataset('category_run_starts', data=category_run_starts)
        f.create_dataset('category_values', data=category_values)

    print(f"✓ Created {filename}")
    print(f"  Rows: {n_rows:,}")
    print(f"  status runs: {len(status_run_starts)} (distribution: 100k, 300k, 200k, 400k)")
    print(f"  category runs: {len(category_run_starts)} (alternating 10/20, 50k each)")
    print(f"  File size: ~16 MB")


if __name__ == '__main__':
    create_large_rse_test()
    print("\n✅ Large RSE test file created successfully!")
