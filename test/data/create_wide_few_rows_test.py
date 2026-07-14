#!/usr/bin/env python3
"""Create a small HDF5 file with wide rows for cache/threading regressions."""

import h5py
import numpy as np


ROWS = 5
HEIGHT = 64
WIDTH = 64
LIST_HEIGHT = 128
LIST_WIDTH = 128
LIST_2D_WIDTH = 8192
EXTREME_SIZE = 2048
SINGLE_WINDOW_ROWS = 9
INT32_ROWS = 300_000
FIXED_CACHE_LIMIT_ROWS = 1_000_001
FIXED_CACHE_LIMIT_WIDTH = 4096
LIST_THREADED_ROWS = 100


def make_wide_data(offset: int) -> np.ndarray:
    row_idx = np.arange(ROWS, dtype=np.float64).reshape(ROWS, 1, 1) * 100_000
    i_idx = np.arange(HEIGHT, dtype=np.float64).reshape(1, HEIGHT, 1) * 100
    j_idx = np.arange(WIDTH, dtype=np.float64).reshape(1, 1, WIDTH)
    return offset + row_idx + i_idx + j_idx


def make_list_data(offset: int) -> np.ndarray:
    row_idx = np.arange(ROWS, dtype=np.float64).reshape(ROWS, 1, 1) * 100_000
    i_idx = np.arange(LIST_HEIGHT, dtype=np.float64).reshape(1, LIST_HEIGHT, 1) * 100
    j_idx = np.arange(LIST_WIDTH, dtype=np.float64).reshape(1, 1, LIST_WIDTH)
    return offset + row_idx + i_idx + j_idx


def write_shape_fixture(filename: str, shape: tuple[int, ...], fillvalue: float) -> None:
    with h5py.File(filename, "w") as f:
        f.create_dataset("values", shape=shape, dtype=np.float64, fillvalue=fillvalue)


with h5py.File("wide_few_rows.h5", "w") as f:
    f.create_dataset("row_id", data=np.arange(ROWS, dtype=np.int32))
    f.create_dataset("zero_inner_large", shape=(3000, 0), dtype=np.int32)
    f.create_dataset(
        "int32_cached",
        data=np.arange(INT32_ROWS, dtype=np.int32),
        chunks=(65_536,),
    )
    f.create_dataset("wide_contiguous", data=make_wide_data(0))
    f.create_dataset("wide_chunked_row", data=make_wide_data(1_000_000), chunks=(1, HEIGHT, WIDTH))
    f.create_dataset("wide_chunked_pair", data=make_wide_data(2_000_000), chunks=(2, HEIGHT, WIDTH))

    f.create_dataset("list_contiguous", data=make_list_data(3_000_000))
    f.create_dataset(
        "list_chunked_row",
        data=make_list_data(3_500_000),
        chunks=(1, LIST_HEIGHT, LIST_WIDTH),
    )
    f.create_dataset(
        "list_chunked_pair",
        data=make_list_data(4_000_000),
        chunks=(2, LIST_HEIGHT, LIST_WIDTH),
    )
    f.create_dataset(
        "list_4d",
        data=np.arange(2 * 8 * 16 * 64, dtype=np.int64).reshape(2, 8, 16, 64),
    )
    f.create_dataset(
        "list_2d",
        data=np.arange(3 * LIST_2D_WIDTH, dtype=np.int64).reshape(3, LIST_2D_WIDTH),
    )
    extreme = np.zeros((1, EXTREME_SIZE, EXTREME_SIZE), dtype=np.int16)
    extreme[0, 0, 0] = 11
    extreme[0, 1023, 1023] = 22
    extreme[0, -1, -1] = 33
    f.create_dataset("extreme_single", data=extreme)
    f.create_dataset(
        "extreme_empty",
        shape=(0, EXTREME_SIZE, EXTREME_SIZE),
        maxshape=(None, EXTREME_SIZE, EXTREME_SIZE),
        dtype=np.int16,
        chunks=(1, EXTREME_SIZE, EXTREME_SIZE),
    )
    f.create_dataset(
        "logical_16gb",
        shape=(2000, 2000, 2000),
        dtype=np.int16,
        fillvalue=-7,
    )
    f.create_dataset(
        "single_cache_window",
        shape=(SINGLE_WINDOW_ROWS, EXTREME_SIZE, EXTREME_SIZE),
        dtype=np.int16,
        chunks=(SINGLE_WINDOW_ROWS, EXTREME_SIZE, EXTREME_SIZE),
        fillvalue=-9,
    )
    f.create_dataset(
        "fixed_cache_limit",
        shape=(FIXED_CACHE_LIMIT_ROWS, FIXED_CACHE_LIMIT_WIDTH),
        dtype=np.uint8,
        chunks=(FIXED_CACHE_LIMIT_ROWS - 1, 32),
        fillvalue=7,
    )
    f.create_dataset(
        "list_threaded",
        shape=(LIST_THREADED_ROWS, LIST_HEIGHT, 64),
        dtype=np.float64,
        chunks=(10, LIST_HEIGHT, 64),
        fillvalue=3.0,
    )

write_shape_fixture("wide_shape_a.h5", (2, LIST_HEIGHT, LIST_WIDTH), 1.0)
write_shape_fixture("wide_shape_b.h5", (3, LIST_HEIGHT, LIST_WIDTH), 2.0)
write_shape_fixture("wide_shape_mismatch.h5", (1, 64, 256), 3.0)

print("Created wide-row test files successfully!")
