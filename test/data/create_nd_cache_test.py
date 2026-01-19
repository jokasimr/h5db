#!/usr/bin/env python3
"""
Create HDF5 datasets for testing N-D chunk caching behavior.
"""

import h5py
import numpy as np


ROWS = 1_000_000


def _fill_dataset(dataset, inner_shape):
    chunk_rows = 10_000
    total = dataset.shape[0]
    for start in range(0, total, chunk_rows):
        count = min(chunk_rows, total - start)
        data = np.arange(start, start + count, dtype=np.int32)
        if inner_shape:
            data = data.reshape((count,) + (1,) * len(inner_shape))
            data = np.broadcast_to(data, (count,) + inner_shape).copy()
        dataset[start : start + count, ...] = data
        if (start // chunk_rows) % 10 == 0:
            print(f"  {dataset.name}: wrote rows {start}..{start + count - 1}")


def create_nd_cache_test_file(filename="nd_cache_test.h5"):
    print(f"Creating {filename} with {ROWS} rows...")

    with h5py.File(filename, "w") as f:
        f.create_dataset("array_2d_contig", shape=(ROWS, 6), dtype=np.int32)
        f.create_dataset("array_2d_chunked_small", shape=(ROWS, 6), dtype=np.int32, chunks=(128, 6))
        f.create_dataset("array_2d_chunked_large", shape=(ROWS, 6), dtype=np.int32, chunks=(4096, 6))
        f.create_dataset("array_2d_chunked_partial", shape=(ROWS, 20), dtype=np.int32, chunks=(10, 5))

        f.create_dataset("tensor_3d_chunked_large", shape=(ROWS, 4, 4), dtype=np.int32, chunks=(4096, 4, 4))
        f.create_dataset("tensor_4d_chunked_small", shape=(ROWS, 2, 2, 2), dtype=np.int32, chunks=(256, 2, 2, 2))

        _fill_dataset(f["array_2d_contig"], (6,))
        _fill_dataset(f["array_2d_chunked_small"], (6,))
        _fill_dataset(f["array_2d_chunked_large"], (6,))
        _fill_dataset(f["array_2d_chunked_partial"], (20,))
        _fill_dataset(f["tensor_3d_chunked_large"], (4, 4))
        _fill_dataset(f["tensor_4d_chunked_small"], (2, 2, 2))

    print(f"OK Created {filename}")


if __name__ == "__main__":
    create_nd_cache_test_file()
