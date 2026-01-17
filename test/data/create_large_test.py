#!/usr/bin/env python3
"""Create a large HDF5 file to test parallel execution."""
import h5py
import numpy as np

# Create dataset with enough rows to trigger parallelism
# DuckDB typically needs >1000 rows to consider parallelization
NUM_ROWS = 100000

print(f"Creating large test file with {NUM_ROWS} rows...")

with h5py.File('large_test.h5', 'w') as f:
    # Create integer dataset
    data = np.arange(NUM_ROWS, dtype=np.int32)
    f.create_dataset('/integers', data=data)

    # Create a second column for more interesting queries
    f.create_dataset('/squares', data=data * data)

    print(f"Created /integers and /squares datasets with {NUM_ROWS} rows each")

print("Done!")
