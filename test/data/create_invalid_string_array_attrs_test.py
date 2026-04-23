#!/usr/bin/env python3
"""Create HDF5 test file with invalid string-array attributes."""

import h5py
import numpy as np


with h5py.File("invalid_string_array_attrs.h5", "w") as f:
    ds = f.create_dataset("invalid_utf8_string_array_attr_dataset", data=np.arange(2, dtype=np.int32))
    ds.attrs.create(
        "invalid_utf8_string_array_attr",
        np.array([b"\xff\xfe"], dtype=h5py.string_dtype(encoding="ascii", length=2)),
        dtype=h5py.string_dtype(encoding="ascii", length=2),
    )

    vlen_ds = f.create_dataset("invalid_utf8_vlen_string_array_attr_dataset", data=np.arange(2, dtype=np.int32))
    vlen_dtype = h5py.string_dtype(encoding="ascii")
    vlen_ds.attrs.create(
        "invalid_utf8_vlen_string_array_attr",
        np.array([b"\xff\xfe"], dtype=vlen_dtype),
        dtype=vlen_dtype,
    )

print("Created invalid_string_array_attrs.h5 successfully!")
