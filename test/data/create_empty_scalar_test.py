#!/usr/bin/env python3
"""Create HDF5 test file with empty and scalar datasets."""

import h5py
import numpy as np


with h5py.File("empty_scalar.h5", "w") as f:
    # Scalar dataset (no dimensions).
    f.create_dataset("scalar_int", data=np.int32(7))

    # Empty datasets.
    f.create_dataset("empty_1d", shape=(0,), dtype=np.int32)
    f.create_dataset("empty_2d", shape=(0, 3), dtype=np.int32)

    # Mismatched lengths for row-count tests.
    f.create_dataset("short", data=np.arange(2, dtype=np.int32))
    f.create_dataset("long", data=np.arange(5, dtype=np.int32))

print("Created empty_scalar.h5 successfully!")
