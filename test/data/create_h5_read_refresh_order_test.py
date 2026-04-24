#!/usr/bin/env python3
"""Create a small chunked fixture for h5_read cache refresh ordering tests."""

from pathlib import Path

import h5py
import numpy as np


ROWS = 512
CHUNK_ROWS = 64


def make_chunk(start: int, count: int, base: int) -> np.ndarray:
    return np.arange(base + start, base + start + count, dtype=np.int32)


output_path = Path(__file__).with_name("h5_read_refresh_order.h5")

with h5py.File(output_path, "w") as f:
    first = f.create_dataset("first", shape=(ROWS,), dtype=np.int32, chunks=(CHUNK_ROWS,))
    second = f.create_dataset("second", shape=(ROWS,), dtype=np.int32, chunks=(CHUNK_ROWS,))

    # Allocate chunk storage in a stable interleaved order: second chunk first, then first chunk.
    for start in range(0, ROWS, CHUNK_ROWS):
        count = min(CHUNK_ROWS, ROWS - start)
        second[start : start + count] = make_chunk(start, count, 10_000)
        first[start : start + count] = make_chunk(start, count, 0)

print(f"Created {output_path.name} successfully!")
