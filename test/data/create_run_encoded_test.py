#!/usr/bin/env python3
"""
Create test HDF5 files with run-length encoded datasets.

Run-length encoding represents repeated values compactly:
- Instead of: [100, 100, 100, 200, 200, 300]
- Store as:
  - run_starts: [0, 3, 5]  (indices where value changes)
  - values: [100, 200, 300]  (the actual values)
"""

import numpy as np
import h5py


def create_run_encoded_test_file(filename='run_encoded.h5'):
    """Create HDF5 file with run-encoded datasets.

    IMPORTANT: The total row count is determined by the "long" regular dataset(s).
    Run-encoded datasets must expand to match this length.
    """
    print(f"Creating {filename}...")

    with h5py.File(filename, 'w') as f:
        # Example 1: Simple group with regular and run-encoded datasets
        grp1 = f.create_group('experiment1')

        # Regular "long" dataset A - this defines the row count (10 rows)
        grp1.create_dataset('timestamp', data=np.arange(10, dtype=np.int64))

        # Run-encoded dataset B (repeated states)
        # Must expand to 10 elements to align with 'timestamp'
        # Logical B: [100, 100, 100, 200, 200, 200, 200, 300, 300, 300]
        grp1.create_dataset('state_run_starts', data=np.array([0, 3, 7], dtype=np.int64))
        grp1.create_dataset('state_values', data=np.array([100, 200, 300], dtype=np.int32))

        # NOTE: No logical_length attribute needed - it's implied by timestamp.shape[0]

        # Example 2: High compression ratio (realistic scenario)
        grp2 = f.create_group('experiment2')

        # Regular "long" datasets - 1000 rows
        grp2.create_dataset('measurement', data=np.random.randn(1000))
        grp2.create_dataset('sensor_id', data=np.random.randint(0, 10, size=1000))

        # Run-encoded status (highly repetitive)
        # Status: 200 × "idle", 300 × "running", 400 × "idle", 100 × "shutdown"
        run_starts = np.array([0, 200, 500, 900], dtype=np.int64)
        run_values = np.array([0, 1, 0, 2], dtype=np.int32)  # 0=idle, 1=running, 2=shutdown

        grp2.create_dataset('status_run_starts', data=run_starts)
        grp2.create_dataset('status_values', data=run_values)

        # Example 3: String values (categorical data)
        grp3 = f.create_group('experiment3')

        # Regular "long" datasets - 8 rows
        grp3.create_dataset('time', data=np.arange(8, dtype=np.float64))
        grp3.create_dataset('value', data=[10.5, 11.2, 12.1, 15.3, 14.8, 16.2, 15.9, 10.1])

        # Run-encoded categorical variable
        # Logical: ["low", "low", "low", "high", "high", "high", "high", "low"]
        run_starts = np.array([0, 3, 7], dtype=np.int64)
        dt = h5py.string_dtype(encoding='utf-8')
        run_values = np.array(["low", "high", "low"], dtype=dt)

        grp3.create_dataset('level_run_starts', data=run_starts)
        grp3.create_dataset('level_values', data=run_values)

        # Example 4: Edge cases
        grp4 = f.create_group('edge_cases')

        # Regular "long" dataset
        grp4.create_dataset('index', data=np.arange(10, dtype=np.int64))

        # Single run (no compression benefit) - must expand to 10
        grp4.create_dataset('single_run_starts', data=np.array([0], dtype=np.int64))
        grp4.create_dataset('single_run_values', data=np.array([42], dtype=np.int32))

        # Every value different (worst case for RLE) - 10 rows, 10 runs
        grp5 = f.create_group('no_compression')
        grp5.create_dataset('seq', data=np.arange(5, dtype=np.int64))
        grp5.create_dataset('bad_rle_starts', data=np.arange(5, dtype=np.int64))
        grp5.create_dataset('bad_rle_values', data=np.array([1, 2, 3, 4, 5], dtype=np.int32))

    print(f"OK Created {filename}")
    print_file_structure(filename)


def print_file_structure(filename):
    """Print structure of HDF5 file."""
    print(f"\nStructure of {filename}:")

    def print_attrs(name, obj):
        indent = '  ' * (name.count('/'))
        if isinstance(obj, h5py.Dataset):
            attrs_str = ""
            if 'encoding' in obj.attrs:
                attrs_str = f" [encoding={obj.attrs['encoding']}"
                if 'logical_length' in obj.attrs:
                    attrs_str += f", len={obj.attrs['logical_length']}"
                attrs_str += "]"
            print(f"{indent}/{name}: shape={obj.shape}, dtype={obj.dtype}{attrs_str}")
        elif isinstance(obj, h5py.Group):
            print(f"{indent}/{name}/")

    with h5py.File(filename, 'r') as f:
        f.visititems(print_attrs)


def demonstrate_expansion():
    """Show how run-encoded data expands."""
    print("\n" + "=" * 70)
    print("Run-Length Encoding Examples")
    print("=" * 70)

    examples = [
        {'name': 'Simple', 'run_starts': [0, 3, 7], 'values': [100, 200, 300], 'length': 10},
        {'name': 'High compression', 'run_starts': [0, 200, 500, 900], 'values': [0, 1, 0, 2], 'length': 1000},
        {'name': 'No compression', 'run_starts': [0, 1, 2, 3, 4], 'values': [10, 20, 30, 40, 50], 'length': 5},
    ]

    for ex in examples:
        run_starts = np.array(ex['run_starts'])
        values = np.array(ex['values'])
        length = ex['length']

        # Expand
        expanded = np.zeros(length, dtype=values.dtype)
        for i in range(len(run_starts)):
            start = run_starts[i]
            end = run_starts[i + 1] if i + 1 < len(run_starts) else length
            expanded[start:end] = values[i]

        # Calculate compression ratio
        compressed_size = len(run_starts) + len(values)
        ratio = length / compressed_size

        print(f"\n{ex['name']}:")
        print(f"  run_starts: {run_starts}")
        print(f"  values: {values}")
        if length <= 20:
            print(f"  expanded: {expanded}")
        else:
            print(f"  expanded (first 10): {expanded[:10]}")
            print(f"  expanded (last 10): {expanded[-10:]}")
        print(f"  Compression: {length} elements -> {compressed_size} values ({ratio:.1f}x)")


if __name__ == '__main__':
    # Show examples
    demonstrate_expansion()

    # Create test file
    print("\n" + "=" * 70)
    create_run_encoded_test_file()

    print("\nOK Run-encoded test file created successfully!")
