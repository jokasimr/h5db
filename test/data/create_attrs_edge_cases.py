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

    # Dataset with invalid UTF-8 payload in a string attribute.
    ds = f.create_dataset("invalid_utf8_attr_dataset", data=np.arange(2, dtype=np.int32))
    ds.attrs.create("invalid_utf8_attr", b"\xff\xfe", dtype=h5py.string_dtype(encoding="ascii", length=2))

    # Dataset with invalid UTF-8 payload in a fixed-length string attribute containing an interior NUL.
    ds = f.create_dataset("invalid_utf8_fixed_attr_dataset", data=np.arange(2, dtype=np.int32))
    ds.attrs.create("invalid_utf8_fixed_attr", b"\xff\x00A", dtype=h5py.string_dtype(encoding="ascii", length=3))

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

    # Dataset with object reference attribute (unsupported type).
    target = f.create_dataset("reference_attr_target", data=np.arange(3, dtype=np.int32))
    ds = f.create_dataset("reference_attr_dataset", data=np.arange(2, dtype=np.int32))
    ds.attrs["ref_attr"] = target.ref

    # Dataset with opaque attribute (unsupported type).
    opaque_dtype = h5py.opaque_dtype(np.dtype("V4"))
    ds = f.create_dataset("opaque_attr_dataset", data=np.arange(2, dtype=np.int32))
    ds.attrs["opaque_attr"] = np.array(np.void(b"qrst"), dtype=opaque_dtype)

    # Dataset with bitfield attribute (unsupported type).
    ds = f.create_dataset("bitfield_attr_dataset", data=np.arange(2, dtype=np.int32))
    attr = h5py.h5a.create(ds.id, b"bitfield_attr", h5py.h5t.STD_B8LE, h5py.h5s.create(h5py.h5s.SCALAR))
    attr.write(np.array(7, dtype=np.uint8))

print("Created attrs_edge_cases.h5 successfully!")
