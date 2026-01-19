#!/usr/bin/env python3
"""
Create test HDF5 files for h5db extension development and testing.
"""

import numpy as np
import h5py


def create_simple_test_file(filename='simple.h5'):
    """Create a simple HDF5 file with basic datasets."""
    print(f"Creating {filename}...")

    # Set seed for reproducible random data
    np.random.seed(42)

    with h5py.File(filename, 'w') as f:
        # Root level datasets
        f.create_dataset('integers', data=np.arange(10, dtype=np.int32))
        f.create_dataset('floats', data=np.random.randn(10).astype(np.float64))
        f.create_dataset('strings', data=np.array([b'hello', b'world', b'test']))

        # 2D dataset
        f.create_dataset('matrix', data=np.arange(20).reshape(5, 4).astype(np.int32))

        # Group with nested datasets
        grp1 = f.create_group('group1')
        grp1.create_dataset('data1', data=np.arange(5, dtype=np.float32))
        grp1.create_dataset('data2', data=np.ones(10, dtype=np.int64))

        # Nested groups
        grp2 = grp1.create_group('subgroup')
        grp2.create_dataset('nested_data', data=np.arange(100, dtype=np.uint8))

        # Add some attributes
        f.attrs['title'] = 'Test HDF5 File'
        f.attrs['version'] = '1.0'
        grp1.attrs['description'] = 'First group'

    print(f"OK Created {filename}")
    print_file_structure(filename)


def create_types_test_file(filename='types.h5'):
    """Create HDF5 file with various data types."""
    print(f"\nCreating {filename}...")

    with h5py.File(filename, 'w') as f:
        # Integer types
        f.create_dataset('int8', data=np.array([1, 2, 3], dtype=np.int8))
        f.create_dataset('int16', data=np.array([100, 200, 300], dtype=np.int16))
        f.create_dataset('int32', data=np.array([10000, 20000], dtype=np.int32))
        f.create_dataset('int64', data=np.array([1000000], dtype=np.int64))

        # Unsigned integers
        f.create_dataset('uint8', data=np.array([255, 254], dtype=np.uint8))
        f.create_dataset('uint16', data=np.array([65535], dtype=np.uint16))
        f.create_dataset('uint32', data=np.array([4294967295], dtype=np.uint32))

        # Floats
        f.create_dataset('float32', data=np.array([3.14, 2.71], dtype=np.float32))
        f.create_dataset('float64', data=np.array([3.141592653589793], dtype=np.float64))

        # Strings
        f.create_dataset('fixed_strings', data=np.array([b'fixed', b'width'], dtype='S5'))
        dt = h5py.string_dtype(encoding='utf-8')
        f.create_dataset('var_strings', data=['variable', 'length', 'strings'], dtype=dt)

    print(f"OK Created {filename}")
    print_file_structure(filename)


def create_multidim_test_file(filename='multidim.h5'):
    """Create HDF5 file with multi-dimensional arrays."""
    print(f"\nCreating {filename}...")

    with h5py.File(filename, 'w') as f:
        # 1D
        f.create_dataset('array_1d', data=np.arange(10))

        # 2D
        f.create_dataset('array_2d', data=np.arange(20).reshape(5, 4))

        # 3D
        f.create_dataset('array_3d', data=np.arange(60).reshape(5, 4, 3))

        # 4D
        f.create_dataset('array_4d', data=np.arange(120).reshape(5, 4, 3, 2))

    print(f"OK Created {filename}")
    print_file_structure(filename)


def print_file_structure(filename):
    """Print the structure of an HDF5 file."""

    def print_attrs(name, obj):
        indent = '  ' * (name.count('/'))
        if isinstance(obj, h5py.Dataset):
            print(f"{indent}/{name}: shape={obj.shape}, dtype={obj.dtype}")
        elif isinstance(obj, h5py.Group):
            print(f"{indent}/{name}/")

    with h5py.File(filename, 'r') as f:
        print(f"\nStructure of {filename}:")
        f.visititems(print_attrs)


if __name__ == '__main__':
    # Create test files
    create_simple_test_file()
    create_types_test_file()
    create_multidim_test_file()

    print("\nOK All test files created successfully!")
    print("\nTest files:")
    print("  - simple.h5    (basic datasets and groups)")
    print("  - types.h5     (various data types)")
    print("  - multidim.h5  (multi-dimensional arrays)")
