import os

import h5py
import numpy as np


def main():
    base_dir = os.path.dirname(__file__)
    path = os.path.join(base_dir, "h5_tree_scalar_deadlock.h5")

    # The old worker could publish one vector ahead of the consumer. Three
    # vectors made it possible for the worker and a downstream scalar HDF5 call
    # to wait on each other. Hard links keep this fixture small.
    with h5py.File(path, "w", libver="latest") as file:
        dataset = file.create_dataset("dataset_0000", data=np.int32(7))
        for index in range(1, 3 * 2_048):
            file[f"dataset_{index:04d}"] = dataset

    print("Created h5_tree_scalar_deadlock.h5 successfully!")


if __name__ == "__main__":
    main()
