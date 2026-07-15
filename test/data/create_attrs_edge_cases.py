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

    # Dataset with a valid fixed-length string attribute containing an interior NUL.
    ds = f.create_dataset("nullpad_attr_dataset", data=np.arange(2, dtype=np.int32))
    ds.attrs.create("nullpad_attr", b"A\x00B", dtype=h5py.string_dtype(encoding="ascii", length=3))

    # Dataset with an invalid fixed-length string attribute after an interior NUL.
    ds = f.create_dataset("invalid_utf8_after_nul_attr_dataset", data=np.arange(2, dtype=np.int32))
    ds.attrs.create("invalid_utf8_after_nul_attr", b"A\x00\xff", dtype=h5py.string_dtype(encoding="ascii", length=3))

    # Dataset with multidimensional attributes.
    ds = f.create_dataset("multidim_attr_dataset", data=np.arange(4, dtype=np.int32))
    ds.attrs["attr_2d"] = np.arange(6, dtype=np.int32).reshape(2, 3)
    ds.attrs["attr_3d"] = np.arange(24, dtype=np.int32).reshape(2, 3, 4)

    # Dataset with maximum supported rank and one unsupported rank.
    ds = f.create_dataset("multidim_rank4_attr_dataset", data=np.arange(1, dtype=np.int32))
    ds.attrs["attr_rank4"] = np.arange(2, dtype=np.int32).reshape(1, 1, 1, 2)
    ds = f.create_dataset("multidim_rank5_attr_dataset", data=np.arange(1, dtype=np.int32))
    ds.attrs["attr_rank5"] = np.arange(1, dtype=np.int32).reshape(1, 1, 1, 1, 1)

    # Dataset with zero-sized multidimensional attributes.
    ds = f.create_dataset("multidim_zero_attr_dataset", data=np.arange(1, dtype=np.int32))
    ds.attrs["zero_0_3"] = np.empty((0, 3), dtype=np.int32)
    ds.attrs["zero_2_0_3"] = np.empty((2, 0, 3), dtype=np.int32)
    ds.attrs["zero_2_3_0"] = np.empty((2, 3, 0), dtype=np.int32)

    # Dataset with multidimensional string attributes.
    ds = f.create_dataset("multidim_string_attr_dataset", data=np.arange(1, dtype=np.int32))
    ds.attrs.create("fixed_text_2d", np.array([[b"a", b"bb"], [b"ccc", b"dddd"]], dtype="S4"))
    vlen_dtype = h5py.string_dtype(encoding="utf-8")
    ds.attrs.create(
        "vlen_text_2d",
        np.array([["a", "bb"], ["ccc", "dddd"]], dtype=object),
        dtype=vlen_dtype,
    )

    # Dataset with invalid UTF-8 inside a multidimensional fixed string attribute.
    ds = f.create_dataset("multidim_invalid_string_attr_dataset", data=np.arange(1, dtype=np.int32))
    ds.attrs.create(
        "invalid_utf8_2d",
        np.array([[b"ok", b"\xff\xfe"]], dtype="S2"),
        dtype=h5py.string_dtype(encoding="ascii", length=2),
    )

    # Dataset with a present attribute that has an H5S_NULL dataspace.
    ds = f.create_dataset("null_dataspace_attr_dataset", data=np.arange(1, dtype=np.int32))
    null_attr_space = h5py.h5s.create(h5py.h5s.NULL)
    h5py.h5a.create(ds.id, b"null_int_attr", h5py.h5t.STD_I32LE, null_attr_space)

    # Dataset with unsupported multidimensional H5T_ARRAY attribute datatype.
    ds = f.create_dataset("multidim_h5t_array_attr_dataset", data=np.arange(1, dtype=np.int32))
    rank2_array_type = h5py.h5t.array_create(h5py.h5t.STD_I32LE, (2, 3))
    scalar_space = h5py.h5s.create(h5py.h5s.SCALAR)
    rank2_array_attr = h5py.h5a.create(ds.id, b"rank2_array_attr", rank2_array_type, scalar_space)
    rank2_array_attr.write(np.arange(6, dtype=np.int32).reshape(2, 3), mtype=rank2_array_type)

    # Dataset with enum attribute (unsupported type).
    ds = f.create_dataset("enum_attr_dataset", data=np.arange(2, dtype=np.int32))
    enum_dtype = h5py.special_dtype(enum=("i1", {"RED": 1, "GREEN": 2}))
    ds.attrs["enum_attr"] = np.array(1, dtype=enum_dtype)

    # Dataset with compound attribute (unsupported type).
    ds = f.create_dataset("compound_attr_dataset", data=np.arange(2, dtype=np.int32))
    compound_dtype = np.dtype([("a", np.int32), ("b", np.float32)])
    ds.attrs["compound_attr"] = np.array((1, 2.5), dtype=compound_dtype)

    # Dataset with float16 attributes (supported by widening to DuckDB FLOAT).
    ds = f.create_dataset("float16_attr_dataset", data=np.arange(2, dtype=np.int32))
    ds.attrs["float16_attr"] = np.float16(1.5)
    ds.attrs["float16_list_attr"] = np.array([1.5, 2.5], dtype=np.float16)

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
