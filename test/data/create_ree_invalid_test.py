#!/usr/bin/env python3
"""Create HDF5 test file with intentionally invalid REE metadata."""

import h5py
import numpy as np


def create_case(group, run_ends, values, run_ends_dtype, values_dtype):
    group.create_dataset("run_ends", data=np.array(run_ends, dtype=run_ends_dtype))
    group.create_dataset("values", data=np.array(values, dtype=values_dtype))


with h5py.File("ree_invalid.h5", "w") as f:
    # Regular dataset for row count (10 rows).
    f.create_dataset("regular", data=np.arange(10, dtype=np.int32))

    # run_ends must not decrease.
    grp = f.create_group("decreasing")
    create_case(grp, [0, 5, 4], [100, 200, 300], np.uint64, np.int32)

    # run_ends and values size mismatch.
    grp = f.create_group("size_mismatch")
    grp.create_dataset("run_ends", data=np.array([0, 5], dtype=np.uint64))
    grp.create_dataset("values", data=np.array([100], dtype=np.int32))

    # run_ends not integer type.
    grp = f.create_group("non_integer_ends")
    create_case(grp, [0.0, 5.0], [100, 200], np.float64, np.int32)

    # run_ends must be 1-dimensional.
    grp = f.create_group("multidim_ends")
    grp.create_dataset("run_ends", data=np.array([[0], [5]], dtype=np.uint64))
    grp.create_dataset("values", data=np.array([100, 200], dtype=np.int32))

    # values must be 1-dimensional.
    grp = f.create_group("multidim_values")
    grp.create_dataset("run_ends", data=np.array([0, 5], dtype=np.uint64))
    grp.create_dataset("values", data=np.array([[100], [200]], dtype=np.int32))


print("Created ree_invalid.h5 successfully!")
