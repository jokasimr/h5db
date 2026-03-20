#!/usr/bin/env python3
"""Create small datasets that exercise cache row-count boundaries."""

from pathlib import Path

import h5py
import numpy as np


ROW_COUNTS = [1, 15, 16, 17, 31, 32, 33]
INNER_SHAPE = (4, 4)
CHUNK_ROWS = 16


def make_data(rows: int, base: int) -> np.ndarray:
    values = np.arange(base, base + rows, dtype=np.int32).reshape(rows, 1, 1)
    return np.broadcast_to(values, (rows,) + INNER_SHAPE).copy()


output_path = Path(__file__).with_name("cache_boundaries.h5")

with h5py.File(output_path, "w") as f:
    for rows in ROW_COUNTS:
        grp = f.create_group(f"rows_{rows}")
        grp.create_dataset("contig", data=make_data(rows, 0))
        grp.create_dataset("chunked", data=make_data(rows, 10_000), chunks=(min(rows, CHUNK_ROWS),) + INNER_SHAPE)

print(f"Created {output_path.name} successfully!")
