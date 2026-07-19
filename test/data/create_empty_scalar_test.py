#!/usr/bin/env python3
"""Create HDF5 test files with empty, scalar, and scalar-read array datasets."""

import h5py
import numpy as np


with h5py.File("empty_scalar.h5", "w") as f:
    # Scalar dataset (no dimensions).
    f.create_dataset("scalar_int", data=np.int32(7))
    f.create_dataset("scalar_str", data="hello")
    f.create_dataset("scalar_long_str", data="x" * 1024)

    # Null dataspaces have no shape or stored value. h5db maps them to
    # broadcast scalar SQL NULL values.
    f.create_dataset("null_int", shape=None, dtype=np.int32)
    f.create_dataset("null_str", shape=None, dtype=h5py.string_dtype(encoding="utf-8"))

    # Variable-length string dataset with empty strings.
    str_dtype = h5py.string_dtype(encoding="utf-8")
    f.create_dataset("null_strings", data=np.array(["", "hello", ""], dtype=object), dtype=str_dtype)

    # Empty datasets.
    f.create_dataset("empty_1d", shape=(0,), dtype=np.int32)
    f.create_dataset("empty_2d", shape=(0, 3), dtype=np.int32)

    # Non-scalar datasets used by scalar h5_read coverage.
    f.create_dataset("array_2d_int", data=np.arange(6, dtype=np.int32).reshape(2, 3))
    f.create_dataset(
        "array_2d_str",
        data=np.array([["alpha", "beta"], ["gamma", "delta"]], dtype=object),
        dtype=h5py.string_dtype(encoding="utf-8"),
    )
    f.create_dataset(
        "array_long_str",
        data=np.array(["x" * 1024], dtype=object),
        dtype=h5py.string_dtype(encoding="utf-8"),
    )
    f.create_dataset("array_many_fixed_str", data=np.full(200_000, b"x", dtype="S1"))
    f.create_dataset("array_5d_uint16", data=np.arange(32, dtype=np.uint16).reshape(2, 2, 2, 2, 2))
    f.create_dataset("array_big_endian_int32", data=np.array([1, 256, 65537], dtype=">i4"))
    f.create_dataset("empty_inner_3d", shape=(2, 0, 3), dtype=np.int32)

    # Mismatched lengths for row-count tests.
    f.create_dataset("short", data=np.arange(2, dtype=np.int32))
    f.create_dataset("long", data=np.arange(5, dtype=np.int32))

    # RSE datasets for row-count validation tests.
    f.create_dataset("rse_run_starts", data=np.array([0, 2], dtype=np.int32))
    f.create_dataset("rse_values", data=np.array([10, 20], dtype=np.int32))

print("Created empty_scalar.h5 successfully!")


def create_multifile_edge_file(filename: str, values: list[int], scalar: int, nullable_scalar: int | None) -> None:
    with h5py.File(filename, "w") as f:
        f.create_dataset("values", data=np.array(values, dtype=np.int32))
        f.create_dataset("scalar", data=np.int32(scalar))
        if nullable_scalar is None:
            f.create_dataset("nullable_scalar", shape=None, dtype=np.int32)
        else:
            f.create_dataset("nullable_scalar", data=np.int32(nullable_scalar))


create_multifile_edge_file("h5_read_multifile_empty_a.h5", [1, 2], 10, 10)
create_multifile_edge_file("h5_read_multifile_empty_b.h5", [], 20, None)
create_multifile_edge_file("h5_read_multifile_empty_c.h5", [10, 20, 30], 30, 30)
print("Created h5_read_multifile_empty_*.h5 successfully!")

with h5py.File("h5_ls_multifile_empty.h5", "w"):
    pass

print("Created h5_ls_multifile_empty.h5 successfully!")
