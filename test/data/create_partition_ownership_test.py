#!/usr/bin/env python3
"""Create a fixture for logical partition ownership boundary tests."""

import h5py
import numpy as np


LOGICAL_PARTITION_SIZE = 10 * 2048
ROW_COUNTS = [
    LOGICAL_PARTITION_SIZE - 1,
    LOGICAL_PARTITION_SIZE,
    LOGICAL_PARTITION_SIZE + 1,
    2 * LOGICAL_PARTITION_SIZE,
    2 * LOGICAL_PARTITION_SIZE + 1,
]


def main() -> None:
    with h5py.File("partition_ownership.h5", "w") as handle:
        for row_count in ROW_COUNTS:
            values = np.arange(row_count, dtype=np.int32)
            chunk_rows = min(row_count, 1024) if row_count > 0 else 1
            handle.create_dataset(f"rows_{row_count}", data=values, chunks=(chunk_rows,))


if __name__ == "__main__":
    main()
