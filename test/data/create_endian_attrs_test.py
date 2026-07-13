#!/usr/bin/env python3
"""Create numeric attributes with explicit little- and big-endian file types."""

import h5py
import numpy as np


with h5py.File("endian_attrs.h5", "w") as f:
    ds = f.create_dataset("values", data=np.arange(2, dtype=np.int32))
    ds.attrs.create("be_int32", np.array(0x01020304, dtype=">i4"), dtype=np.dtype(">i4"))
    ds.attrs.create("le_int32", np.array(0x01020304, dtype="<i4"), dtype=np.dtype("<i4"))
    ds.attrs.create("be_float64", np.array(1234.5, dtype=">f8"), dtype=np.dtype(">f8"))
    ds.attrs.create("le_float64", np.array(1234.5, dtype="<f8"), dtype=np.dtype("<f8"))
    ds.attrs.create(
        "be_int32_list",
        np.array([1, 256, 65537], dtype=">i4"),
        dtype=np.dtype(">i4"),
    )
    ds.attrs.create(
        "le_int32_list",
        np.array([1, 256, 65537], dtype="<i4"),
        dtype=np.dtype("<i4"),
    )

    scalar_space = h5py.h5s.create(h5py.h5s.SCALAR)
    native_array_type = h5py.h5t.array_create(h5py.h5t.NATIVE_INT32, (3,))
    for name, file_base_type in (
        (b"be_int32_h5t_array", h5py.h5t.STD_I32BE),
        (b"le_int32_h5t_array", h5py.h5t.STD_I32LE),
    ):
        file_array_type = h5py.h5t.array_create(file_base_type, (3,))
        attr = h5py.h5a.create(ds.id, name, file_array_type, scalar_space)
        attr.write(np.array([1, 256, 65537], dtype=np.int32), mtype=native_array_type)

print("Created endian_attrs.h5 successfully!")
