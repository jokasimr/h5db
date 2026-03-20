#!/usr/bin/env python3
"""Generate HDF5 fixture for sparse pushdown across logical partition boundaries."""

from pathlib import Path

import h5py
import numpy as np


PARTITION_SIZE = 10 * 2048
ROWS = 4 * PARTITION_SIZE + 10


def main() -> None:
    values = np.arange(ROWS, dtype=np.int32)
    labels = np.array([f"row_{i:05d}".encode() for i in range(ROWS)], dtype="S10")

    selector_starts = np.array(
        [
            0,
            PARTITION_SIZE - 2,
            PARTITION_SIZE + 3,
            2 * PARTITION_SIZE + 5,
            2 * PARTITION_SIZE + 7,
            2 * PARTITION_SIZE + 10,
            2 * PARTITION_SIZE + 12,
            4 * PARTITION_SIZE + 5,
        ],
        dtype=np.uint64,
    )
    selector_values = np.array([0, 1, 0, 1, 0, 1, 0, 1], dtype=np.int32)

    output_path = Path(__file__).with_name("sparse_partition_pushdown.h5")
    with h5py.File(output_path, "w") as handle:
        handle.create_dataset("/value_contig", data=values)
        handle.create_dataset("/value_chunked", data=values, chunks=(1024,))
        handle.create_dataset("/label", data=labels)
        handle.create_dataset("/selector_starts", data=selector_starts)
        handle.create_dataset("/selector_values", data=selector_values)


if __name__ == "__main__":
    main()
