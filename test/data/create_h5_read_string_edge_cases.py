#!/usr/bin/env python3
"""Create HDF5 test file with h5_read string dataset edge cases."""

import h5py
import numpy as np


def create_fixed_string_dataset(parent_id, name, values, size, strpad):
    tid = h5py.h5t.C_S1.copy()
    tid.set_size(size)
    tid.set_cset(h5py.h5t.CSET_ASCII)
    tid.set_strpad(strpad)

    sid = h5py.h5s.create_simple((len(values),))
    ds = h5py.h5d.create(parent_id, name.encode(), tid, sid)
    ds.write(h5py.h5s.ALL, h5py.h5s.ALL, np.array(values, dtype=f"S{size}"))


def create_fixed_string_scalar(parent_id, name, value, size, strpad):
    tid = h5py.h5t.C_S1.copy()
    tid.set_size(size)
    tid.set_cset(h5py.h5t.CSET_ASCII)
    tid.set_strpad(strpad)

    sid = h5py.h5s.create(h5py.h5s.SCALAR)
    ds = h5py.h5d.create(parent_id, name.encode(), tid, sid)
    ds.write(h5py.h5s.ALL, h5py.h5s.ALL, np.array(value, dtype=f"S{size}"))


with h5py.File("h5_read_string_edge_cases.h5", "w") as f:
    create_fixed_string_dataset(
        f.id,
        "spacepad_fixed",
        [b"a", b"xy"],
        size=5,
        strpad=h5py.h5t.STR_SPACEPAD,
    )
    create_fixed_string_scalar(
        f.id,
        "scalar_spacepad_fixed",
        b"a",
        size=5,
        strpad=h5py.h5t.STR_SPACEPAD,
    )
    create_fixed_string_dataset(
        f.id,
        "nullterm_fixed",
        [b"a", b"xy"],
        size=5,
        strpad=h5py.h5t.STR_NULLTERM,
    )

    create_fixed_string_dataset(
        f.id,
        "nullpad_fixed_interior_nul",
        [b"A\x00B"],
        size=3,
        strpad=h5py.h5t.STR_NULLPAD,
    )
    create_fixed_string_scalar(
        f.id,
        "scalar_nullpad_fixed_interior_nul",
        b"A\x00B",
        size=3,
        strpad=h5py.h5t.STR_NULLPAD,
    )
    create_fixed_string_dataset(
        f.id,
        "nullpad_fixed_invalid_after_nul",
        [b"A\x00\xff"],
        size=3,
        strpad=h5py.h5t.STR_NULLPAD,
    )
    create_fixed_string_dataset(
        f.id,
        "fixed_invalid_no_nul",
        [b"\xff\xfe"],
        size=2,
        strpad=h5py.h5t.STR_NULLPAD,
    )

    ascii_vlen = h5py.string_dtype(encoding="ascii")
    f.create_dataset(
        "invalid_utf8_var",
        data=np.array([b"\xff\xfe"], dtype=object),
        dtype=ascii_vlen,
    )
    f.create_dataset(
        "scalar_invalid_utf8_var",
        data=np.array(b"\xff\xfe", dtype=object),
        dtype=ascii_vlen,
    )

    grp = f.create_group("rse_invalid")
    grp.create_dataset("index", data=np.arange(3, dtype=np.int32))
    grp.create_dataset("run_starts", data=np.array([0], dtype=np.uint64))
    create_fixed_string_dataset(
        grp.id,
        "values",
        [b"\xff\xfe"],
        size=2,
        strpad=h5py.h5t.STR_NULLPAD,
    )

print("Created h5_read_string_edge_cases.h5 successfully!")
