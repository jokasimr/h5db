#!/usr/bin/env python3
"""Create a fixture for h5_read cache-progress boundary tests."""

import h5py
import numpy as np


CACHE_PROGRESS_BOUNDARY = 10 * 2048
ROW_COUNTS = [
    CACHE_PROGRESS_BOUNDARY - 1,
    CACHE_PROGRESS_BOUNDARY,
    CACHE_PROGRESS_BOUNDARY + 1,
    2 * CACHE_PROGRESS_BOUNDARY,
    2 * CACHE_PROGRESS_BOUNDARY + 1,
]


def main() -> None:
    with h5py.File("cache_progress.h5", "w") as handle:
        for row_count in ROW_COUNTS:
            values = np.arange(row_count, dtype=np.int32)
            chunk_rows = min(row_count, 1024) if row_count > 0 else 1
            handle.create_dataset(f"rows_{row_count}", data=values, chunks=(chunk_rows,))


if __name__ == "__main__":
    main()
