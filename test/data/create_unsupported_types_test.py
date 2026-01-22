#!/usr/bin/env python3
"""Create HDF5 test file with unsupported dataset types."""

import h5py
import numpy as np


with h5py.File("unsupported_types.h5", "w") as f:
    # Float16 dataset (unsupported float size).
    f.create_dataset("float16_values", data=np.array([1.5, 2.5], dtype=np.float16))

    # Enum dataset.
    enum_dtype = h5py.special_dtype(enum=("i1", {"RED": 1, "GREEN": 2, "BLUE": 3}))
    f.create_dataset("enum_values", data=np.array([1, 2, 3], dtype=np.int8), dtype=enum_dtype)

    # Compound dataset.
    compound_dtype = np.dtype([("a", np.int32), ("b", np.float32)])
    compound_data = np.array([(1, 1.25), (2, 2.5)], dtype=compound_dtype)
    f.create_dataset("compound_values", data=compound_data)

print("Created unsupported_types.h5 successfully!")
