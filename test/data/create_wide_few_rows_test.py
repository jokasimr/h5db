#!/usr/bin/env python3
"""Create a small HDF5 file with wide rows for cache/threading regressions."""

import h5py
import numpy as np


ROWS = 5
HEIGHT = 64
WIDTH = 64


def make_wide_data(offset: int) -> np.ndarray:
    row_idx = np.arange(ROWS, dtype=np.float64).reshape(ROWS, 1, 1) * 100_000
    i_idx = np.arange(HEIGHT, dtype=np.float64).reshape(1, HEIGHT, 1) * 100
    j_idx = np.arange(WIDTH, dtype=np.float64).reshape(1, 1, WIDTH)
    return offset + row_idx + i_idx + j_idx


with h5py.File("wide_few_rows.h5", "w") as f:
    f.create_dataset("row_id", data=np.arange(ROWS, dtype=np.int32))
    f.create_dataset("wide_contiguous", data=make_wide_data(0))
    f.create_dataset("wide_chunked_row", data=make_wide_data(1_000_000), chunks=(1, HEIGHT, WIDTH))
    f.create_dataset("wide_chunked_pair", data=make_wide_data(2_000_000), chunks=(2, HEIGHT, WIDTH))

print("Created wide_few_rows.h5 successfully!")
