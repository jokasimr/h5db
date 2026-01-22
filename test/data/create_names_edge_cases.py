#!/usr/bin/env python3
"""Create HDF5 test file with dataset names containing spaces/unicode."""

import h5py
import numpy as np


with h5py.File("names_edge_cases.h5", "w") as f:
    f.create_dataset("with space", data=np.arange(3, dtype=np.int32))

    grp = f.create_group("group with space")
    grp.create_dataset("data", data=np.arange(2, dtype=np.int32))

    f.create_dataset("unicode_µ", data=np.arange(4, dtype=np.int32))

print("Created names_edge_cases.h5 successfully!")
