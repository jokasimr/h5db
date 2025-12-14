#!/usr/bin/env python3
"""Create HDF5 test file with various attributes."""

import h5py
import numpy as np

# Create test file
with h5py.File('with_attrs.h5', 'w') as f:
    # Create a dataset with scalar attributes
    ds = f.create_dataset('dataset_with_attrs', data=np.arange(10))

    # Scalar integer attributes (different sizes and signs)
    ds.attrs['int8_attr'] = np.int8(42)
    ds.attrs['int16_attr'] = np.int16(1234)
    ds.attrs['int32_attr'] = np.int32(123456)
    ds.attrs['int64_attr'] = np.int64(9876543210)
    ds.attrs['uint8_attr'] = np.uint8(255)
    ds.attrs['uint16_attr'] = np.uint16(65535)
    ds.attrs['uint32_attr'] = np.uint32(4294967295)
    ds.attrs['uint64_attr'] = np.uint64(18446744073709551615)

    # Scalar float attributes
    ds.attrs['float32_attr'] = np.float32(3.14159)
    ds.attrs['float64_attr'] = np.float64(2.718281828)

    # String attribute
    ds.attrs['string_attr'] = 'Hello HDF5!'

    # Array attributes (1D)
    ds.attrs['int_array_attr'] = np.array([1, 2, 3, 4, 5], dtype=np.int32)
    ds.attrs['float_array_attr'] = np.array([1.1, 2.2, 3.3], dtype=np.float64)

    # Create a group with attributes
    grp = f.create_group('group_with_attrs')
    grp.attrs['group_int_attr'] = np.int64(999)
    grp.attrs['group_string_attr'] = 'I am a group'
    grp.attrs['group_array_attr'] = np.array([10, 20, 30], dtype=np.int64)

print("Created with_attrs.h5 successfully!")
