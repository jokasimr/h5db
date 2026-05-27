#!/usr/bin/env python3
"""Create small datasets that exercise cache row-count boundaries."""

from pathlib import Path

import h5py
import numpy as np


ROW_COUNTS = [1, 15, 16, 17, 31, 32, 33]
INNER_SHAPE = (4, 4)
CHUNK_ROWS = 16
EARLY_STOP_ROWS = 70_000
MIXED_WIDTH_ROWS = 40


def make_data(rows: int, base: int) -> np.ndarray:
    values = np.arange(base, base + rows, dtype=np.int32).reshape(rows, 1, 1)
    return np.broadcast_to(values, (rows,) + INNER_SHAPE).copy()


output_path = Path(__file__).with_name("cache_boundaries.h5")

with h5py.File(output_path, "w") as f:
    for rows in ROW_COUNTS:
        grp = f.create_group(f"rows_{rows}")
        grp.create_dataset("contig", data=make_data(rows, 0))
        grp.create_dataset("chunked", data=make_data(rows, 10_000), chunks=(min(rows, CHUNK_ROWS),) + INNER_SHAPE)

    early_stop = f.create_group("early_stop_parallel")
    early_stop.create_dataset("value", data=np.arange(EARLY_STOP_ROWS, dtype=np.int64), chunks=(1024,))

    mixed_width = f.create_group("mixed_width")
    mixed_width.create_dataset(
        "wide",
        data=make_data(MIXED_WIDTH_ROWS, 20_000),
        chunks=(CHUNK_ROWS,) + INNER_SHAPE,
    )
    mixed_width.create_dataset("narrow", data=np.arange(MIXED_WIDTH_ROWS, dtype=np.int64), chunks=(CHUNK_ROWS,))

print(f"Created {output_path.name} successfully!")
