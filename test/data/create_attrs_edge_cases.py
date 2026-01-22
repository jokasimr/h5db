#!/usr/bin/env python3
"""Create HDF5 test file with attribute edge cases."""

import h5py
import numpy as np


with h5py.File("attrs_edge_cases.h5", "w") as f:
    # Dataset with no attributes.
    f.create_dataset("empty_dataset", data=np.arange(3, dtype=np.int32))

    # Group with no attributes.
    f.create_group("empty_group")

    # Dataset with variable-length and fixed-length string attributes.
    ds = f.create_dataset("string_attrs", data=np.arange(2, dtype=np.int32))
    ds.attrs["vlen_str_attr"] = "variable"
    ds.attrs["fixed_str_attr"] = np.bytes_("fixed")

    # Dataset with unsupported multidimensional attribute (2D).
    ds = f.create_dataset("multidim_attr_dataset", data=np.arange(4, dtype=np.int32))
    ds.attrs["attr_2d"] = np.arange(6, dtype=np.int32).reshape(2, 3)

    # Dataset with enum attribute (unsupported type).
    ds = f.create_dataset("enum_attr_dataset", data=np.arange(2, dtype=np.int32))
    enum_dtype = h5py.special_dtype(enum=("i1", {"RED": 1, "GREEN": 2}))
    ds.attrs["enum_attr"] = np.array(1, dtype=enum_dtype)

    # Dataset with compound attribute (unsupported type).
    ds = f.create_dataset("compound_attr_dataset", data=np.arange(2, dtype=np.int32))
    compound_dtype = np.dtype([("a", np.int32), ("b", np.float32)])
    ds.attrs["compound_attr"] = np.array((1, 2.5), dtype=compound_dtype)

    # Dataset with float16 attribute (unsupported float size).
    ds = f.create_dataset("float16_attr_dataset", data=np.arange(2, dtype=np.int32))
    ds.attrs["float16_attr"] = np.float16(1.5)

print("Created attrs_edge_cases.h5 successfully!")
