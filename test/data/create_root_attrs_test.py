#!/usr/bin/env python3
"""Create HDF5 test file with root attributes and unsupported string-array attrs."""

import h5py
import numpy as np


with h5py.File("root_attrs.h5", "w") as f:
    f.attrs["root_name"] = "root"
    f.attrs["root_number"] = np.int32(7)

    ds = f.create_dataset("string_array_attrs", data=np.arange(3, dtype=np.int32))
    string_dtype = h5py.string_dtype(encoding="utf-8")
    ds.attrs.create("labels", np.array(["a", "bb", "ccc"], dtype=string_dtype), dtype=string_dtype)

print("Created root_attrs.h5 successfully!")
