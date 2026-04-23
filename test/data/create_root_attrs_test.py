#!/usr/bin/env python3
"""Create HDF5 test file with root attributes and 1D array attributes."""

import h5py
import numpy as np


with h5py.File("root_attrs.h5", "w") as f:
    f.attrs["root_name"] = "root"
    f.attrs["root_number"] = np.int32(7)

    ds = f.create_dataset("string_array_attrs", data=np.arange(3, dtype=np.int32))
    string_dtype = h5py.string_dtype(encoding="utf-8")
    ds.attrs.create("labels", np.array(["a", "bb", "ccc"], dtype=string_dtype), dtype=string_dtype)

    ds = f.create_dataset("empty_array_attrs", data=np.arange(1, dtype=np.int32))
    ds.attrs["empty_numbers"] = np.array([], dtype=np.int32)
    ds.attrs.create("empty_labels", np.array([], dtype=string_dtype), dtype=string_dtype)

    ds = f.create_dataset("h5t_array_attrs", data=np.arange(1, dtype=np.int32))

    int_array_type = h5py.h5t.array_create(h5py.h5t.NATIVE_INT32, (3,))
    scalar_space = h5py.h5s.create(h5py.h5s.SCALAR)
    fixed_numbers = h5py.h5a.create(ds.id, b"fixed_numbers", int_array_type, scalar_space)
    fixed_numbers.write(np.array([1, 2, 3], dtype=np.int32), mtype=int_array_type)

    fixed_string_type = h5py.h5t.C_S1.copy()
    fixed_string_type.set_size(3)
    fixed_string_type.set_cset(h5py.h5t.CSET_UTF8)
    fixed_string_type.set_strpad(h5py.h5t.STR_NULLTERM)
    string_array_type = h5py.h5t.array_create(fixed_string_type, (3,))
    fixed_labels = h5py.h5a.create(ds.id, b"fixed_labels", string_array_type, scalar_space)
    fixed_labels.write(np.array([b"a", b"bb", b"ccc"], dtype="S3"), mtype=string_array_type)

print("Created root_attrs.h5 successfully!")
