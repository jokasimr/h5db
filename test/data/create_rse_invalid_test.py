#!/usr/bin/env python3
"""Create HDF5 test file with intentionally invalid RSE metadata."""

import h5py
import numpy as np


def create_case(group, run_starts, values, run_starts_dtype, values_dtype):
    group.create_dataset("run_starts", data=np.array(run_starts, dtype=run_starts_dtype))
    group.create_dataset("values", data=np.array(values, dtype=values_dtype))


with h5py.File("rse_invalid.h5", "w") as f:
    # Regular dataset for row count (10 rows).
    f.create_dataset("regular", data=np.arange(10, dtype=np.int32))

    # run_starts not strictly increasing (duplicate).
    grp = f.create_group("not_increasing")
    create_case(grp, [0, 5, 5], [100, 200, 300], np.uint64, np.int32)

    # run_starts contains index beyond dataset length.
    grp = f.create_group("exceeds_length")
    create_case(grp, [0, 11], [100, 200], np.uint64, np.int32)

    # run_starts and values size mismatch.
    grp = f.create_group("size_mismatch")
    grp.create_dataset("run_starts", data=np.array([0, 5], dtype=np.uint64))
    grp.create_dataset("values", data=np.array([100], dtype=np.int32))

    # run_starts not integer type.
    grp = f.create_group("non_integer_starts")
    create_case(grp, [0.0, 5.0], [100, 200], np.float64, np.int32)

print("Created rse_invalid.h5 successfully!")
