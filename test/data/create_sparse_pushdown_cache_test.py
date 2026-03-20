#!/usr/bin/env python3
"""
Generate HDF5 test file for sparse pushdown ranges over cached regular columns.
"""

from pathlib import Path

import h5py
import numpy as np


def main():
    rows = 33

    row_values = np.arange(rows, dtype=np.int32).reshape(rows, 1, 1)
    regular = np.broadcast_to(row_values, (rows, 4, 4)).copy()
    labels = np.array([f"row_{i:02d}".encode() for i in range(rows)], dtype="S6")

    selector_starts = np.array([0, 8, 16, 24, 32], dtype=np.uint64)
    selector_values = np.array([1, 0, 1, 0, 1], dtype=np.int32)

    output_path = Path(__file__).with_name("sparse_pushdown_cache.h5")
    with h5py.File(output_path, "w") as f:
        f.create_dataset("/regular_contig", data=regular)
        f.create_dataset("/regular_chunked", data=regular, chunks=(16, 4, 4))
        f.create_dataset("/label", data=labels)
        f.create_dataset("/selector_starts", data=selector_starts)
        f.create_dataset("/selector_values", data=selector_values)

    print(f"Created {output_path.name} successfully!")
    print("  - regular_contig: int32[33][4][4]")
    print("  - regular_chunked: int32[33][4][4] with chunks=(16,4,4)")
    print("  - label: fixed-length string labels row_00..row_32")
    print("  - selector RSE runs: [0,8)=1, [8,16)=0, [16,24)=1, [24,32)=0, [32,33)=1")


if __name__ == "__main__":
    main()
