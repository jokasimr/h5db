#!/usr/bin/env python3
"""Create HDF5 test file with REE edge cases."""

import h5py
import numpy as np


def create_ree_dataset(f, group_name, index_data, run_ends, values, dtype=np.int32):
    """Helper to create REE dataset with index, run_ends, and values."""
    grp = f.create_group(group_name)
    grp.create_dataset('index', data=index_data)
    grp.create_dataset('run_ends', data=run_ends, dtype=np.uint64)
    grp.create_dataset('values', data=values, dtype=dtype)
    return grp


with h5py.File('ree_edge_cases.h5', 'w') as f:
    create_ree_dataset(
        f,
        'basic',
        index_data=np.arange(6, dtype=np.int32),
        run_ends=np.array([1, 3, 5], dtype=np.uint64),
        values=np.array([10, 20, 30], dtype=np.int32),
    )

    create_ree_dataset(
        f,
        'trailing_nulls',
        index_data=np.arange(6, dtype=np.int32),
        run_ends=np.array([2, 4], dtype=np.uint64),
        values=np.array([10, 20], dtype=np.int32),
    )

    create_ree_dataset(
        f,
        'empty_runs',
        index_data=np.arange(5, dtype=np.int32),
        run_ends=np.array([], dtype=np.uint64),
        values=np.array([], dtype=np.int32),
    )

    create_ree_dataset(
        f,
        'single_row_first_end',
        index_data=np.arange(4, dtype=np.int32),
        run_ends=np.array([0, 3], dtype=np.uint64),
        values=np.array([10, 20], dtype=np.int32),
    )

    create_ree_dataset(
        f,
        'zero_length_middle',
        index_data=np.arange(6, dtype=np.int32),
        run_ends=np.array([2, 2, 4], dtype=np.uint64),
        values=np.array([10, 20, 30], dtype=np.int32),
    )

    create_ree_dataset(
        f,
        'zero_length_end',
        index_data=np.arange(6, dtype=np.int32),
        run_ends=np.array([2, 4, 4], dtype=np.uint64),
        values=np.array([10, 20, 99], dtype=np.int32),
    )

    create_ree_dataset(
        f,
        'zero_length_repeated',
        index_data=np.arange(6, dtype=np.int32),
        run_ends=np.array([1, 3, 3, 3], dtype=np.uint64),
        values=np.array([10, 20, 30, 40], dtype=np.int32),
    )

    create_ree_dataset(
        f,
        'string_trailing_nulls',
        index_data=np.arange(5, dtype=np.int32),
        run_ends=np.array([1, 1, 3], dtype=np.uint64),
        values=np.array(['alpha', 'unused', 'beta'], dtype=h5py.string_dtype()),
        dtype=h5py.string_dtype(),
    )

    grp = f.create_group('mixed_pushdown')
    grp.create_dataset('index', data=np.arange(10, dtype=np.int32))
    grp.create_dataset('rse_starts', data=np.array([0, 4, 7], dtype=np.uint64))
    grp.create_dataset('rse_values', data=np.array([1, 2, 3], dtype=np.int32))
    grp.create_dataset('ree_ends', data=np.array([2, 5, 8], dtype=np.uint64))
    grp.create_dataset('ree_values', data=np.array([10, 20, 30], dtype=np.int32))

    create_ree_dataset(
        f,
        'overlong_tail',
        index_data=np.arange(5, dtype=np.int32),
        run_ends=np.array([1, 5, 100], dtype=np.uint64),
        values=np.array([10, 20, 99], dtype=np.int32),
    )

    create_ree_dataset(
        f,
        'overlong_only',
        index_data=np.arange(5, dtype=np.int32),
        run_ends=np.array([100], dtype=np.uint64),
        values=np.array([20], dtype=np.int32),
    )

    create_ree_dataset(
        f,
        'overlong_uint64_max',
        index_data=np.arange(5, dtype=np.int32),
        run_ends=np.array([0, np.iinfo(np.uint64).max], dtype=np.uint64),
        values=np.array([10, 20], dtype=np.int32),
    )


print("Created ree_edge_cases.h5 successfully!")
