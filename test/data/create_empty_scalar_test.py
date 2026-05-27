#!/usr/bin/env python3
"""Create HDF5 test files with empty and scalar datasets."""

import h5py
import numpy as np


with h5py.File("empty_scalar.h5", "w") as f:
    # Scalar dataset (no dimensions).
    f.create_dataset("scalar_int", data=np.int32(7))
    f.create_dataset("scalar_str", data="hello")

    # Variable-length string dataset with empty strings.
    str_dtype = h5py.string_dtype(encoding="utf-8")
    f.create_dataset("null_strings", data=np.array(["", "hello", ""], dtype=object), dtype=str_dtype)

    # Empty datasets.
    f.create_dataset("empty_1d", shape=(0,), dtype=np.int32)
    f.create_dataset("empty_2d", shape=(0, 3), dtype=np.int32)

    # Mismatched lengths for row-count tests.
    f.create_dataset("short", data=np.arange(2, dtype=np.int32))
    f.create_dataset("long", data=np.arange(5, dtype=np.int32))

    # RSE datasets for row-count validation tests.
    f.create_dataset("rse_run_starts", data=np.array([0, 2], dtype=np.int32))
    f.create_dataset("rse_values", data=np.array([10, 20], dtype=np.int32))

print("Created empty_scalar.h5 successfully!")


def create_multifile_edge_file(filename: str, values: list[int], scalar: int) -> None:
    with h5py.File(filename, "w") as f:
        f.create_dataset("values", data=np.array(values, dtype=np.int32))
        f.create_dataset("scalar", data=np.int32(scalar))


create_multifile_edge_file("h5_read_multifile_empty_a.h5", [1, 2], 10)
create_multifile_edge_file("h5_read_multifile_empty_b.h5", [], 20)
create_multifile_edge_file("h5_read_multifile_empty_c.h5", [10, 20, 30], 30)
print("Created h5_read_multifile_empty_*.h5 successfully!")
