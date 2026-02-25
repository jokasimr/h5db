#!/usr/bin/env python3
"""Create HDF5 test file with many groups and tiny datasets."""

import h5py
import numpy as np


NUM_GROUPS = 100_000


def main(filename="many_groups.h5"):
    print(f"Creating {filename} with {NUM_GROUPS} groups...")
    with h5py.File(filename, "w") as f:
        for i in range(NUM_GROUPS):
            grp = f.create_group(f"g{i:06d}")
            if i % 3 == 0:
                grp.create_dataset("scalar", data=np.int32(i))
            elif i % 3 == 1:
                grp.create_dataset("vec", data=np.array([i], dtype=np.int32))
            else:
                grp.create_dataset("mat", data=np.arange(2, dtype=np.int32).reshape(1, 2))
    print(f"Created {filename} successfully!")


if __name__ == "__main__":
    main()
