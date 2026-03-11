#!/usr/bin/env python3
"""Create HDF5 test file with unsupported dataset types."""

import h5py
import numpy as np


with h5py.File("unsupported_types.h5", "w") as f:
    # Float16 dataset (unsupported float size).
    f.create_dataset("float16_values", data=np.array([1.5, 2.5], dtype=np.float16))

    # Enum dataset.
    enum_dtype = h5py.special_dtype(enum=("i1", {"RED": 1, "GREEN": 2, "BLUE": 3}))
    f.create_dataset("enum_values", data=np.array([1, 2, 3], dtype=np.int8), dtype=enum_dtype)

    # Compound dataset.
    compound_dtype = np.dtype([("a", np.int32), ("b", np.float32)])
    compound_data = np.array([(1, 1.25), (2, 2.5)], dtype=compound_dtype)
    f.create_dataset("compound_values", data=compound_data)

    # Object reference dataset.
    target = f.create_dataset("reference_target", data=np.arange(3, dtype=np.int32))
    references = f.create_dataset("reference_values", shape=(1,), dtype=h5py.ref_dtype)
    references[0] = target.ref

    # Variable-length integer dataset.
    vlen_int_dtype = h5py.vlen_dtype(np.dtype("int32"))
    vlen_values = np.empty(2, dtype=object)
    vlen_values[0] = np.array([1, 2], dtype=np.int32)
    vlen_values[1] = np.array([3], dtype=np.int32)
    f.create_dataset("vlen_int_values", data=vlen_values, dtype=vlen_int_dtype)

    # Opaque dataset.
    opaque_dtype = h5py.opaque_dtype(np.dtype("V4"))
    opaque_values = np.array([np.void(b"abcd"), np.void(b"WXYZ")], dtype=opaque_dtype)
    f.create_dataset("opaque_values", data=opaque_values, dtype=opaque_dtype)

    # Bitfield dataset.
    bitfield_space = h5py.h5s.create_simple((3,))
    bitfield_dataset = h5py.h5d.create(f.id, b"bitfield_values", h5py.h5t.STD_B8LE, bitfield_space)
    bitfield_dataset.write(h5py.h5s.ALL, h5py.h5s.ALL, np.array([1, 2, 255], dtype=np.uint8))

print("Created unsupported_types.h5 successfully!")
