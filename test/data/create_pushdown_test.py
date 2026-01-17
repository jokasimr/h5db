#!/usr/bin/env python3
"""
Generate HDF5 test file for predicate pushdown testing.

This file creates RSE columns with both sorted and unsorted values
to test the predicate pushdown optimization.
"""

import h5py
import numpy as np


def main():
    print("Creating pushdown_test.h5...")

    with h5py.File('pushdown_test.h5', 'w') as f:
        # RSE: 1000 rows with SORTED numeric values [10, 20, 30, 40, 50]
        # Each run has 200 rows
        run_starts = np.array([0, 200, 400, 600, 800], dtype=np.uint64)
        values_int_sorted = np.array([10, 20, 30, 40, 50], dtype=np.int32)
        f.create_dataset('/int_rse_starts', data=run_starts)
        f.create_dataset('/int_rse_values', data=values_int_sorted)

        # RSE with UNSORTED values (should NOT be optimized)
        values_int_unsorted = np.array([50, 10, 30, 20, 40], dtype=np.int32)
        f.create_dataset('/int_unsorted_rse_starts', data=run_starts)
        f.create_dataset('/int_unsorted_rse_values', data=values_int_unsorted)

        # RSE with float values for float testing (sorted)
        values_float = np.array([1.5, 2.5, 3.5, 4.5, 5.5], dtype=np.float32)
        f.create_dataset('/float_rse_starts', data=run_starts)
        f.create_dataset('/float_rse_values', data=values_float)

        # RSE with int64 values (sorted)
        values_int64 = np.array([100, 200, 300, 400, 500], dtype=np.int64)
        f.create_dataset('/int64_rse_starts', data=run_starts)
        f.create_dataset('/int64_rse_values', data=values_int64)

        # RSE with string values (should NOT be optimized - strings excluded)
        values_string = np.array([b'alpha', b'beta', b'gamma', b'delta', b'epsilon'], dtype='S10')
        f.create_dataset('/string_rse_starts', data=run_starts)
        f.create_dataset('/string_rse_values', data=values_string)

        # Regular column for comparison (0-999)
        regular = np.arange(1000, dtype=np.int32)
        f.create_dataset('/regular', data=regular)

        # Another regular column with float values
        regular_float = np.arange(1000, dtype=np.float64) * 0.1
        f.create_dataset('/regular_float', data=regular_float)

    print("Created pushdown_test.h5 with:")
    print("  - Sorted int32 RSE column (values: 10, 20, 30, 40, 50)")
    print("  - Unsorted int32 RSE column (values: 50, 10, 30, 20, 40)")
    print("  - Sorted float32 RSE column (values: 1.5, 2.5, 3.5, 4.5, 5.5)")
    print("  - Sorted int64 RSE column (values: 100, 200, 300, 400, 500)")
    print("  - String RSE column (not optimized)")
    print("  - Regular int32 column (0-999)")
    print("  - Regular float64 column (0.0-99.9)")
    print("  - Each RSE run spans 200 rows")


if __name__ == '__main__':
    main()
