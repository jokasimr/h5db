#!/usr/bin/env python3
"""Create HDF5 test file with mismatched multidim row counts."""

import h5py
import numpy as np


with h5py.File("multidim_mismatch.h5", "w") as f:
    f.create_dataset("array_2d", data=np.arange(12).reshape(4, 3))
    f.create_dataset("array_3d", data=np.arange(24).reshape(6, 2, 2))

print("Created multidim_mismatch.h5 successfully!")
