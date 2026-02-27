import os
import h5py
import numpy as np


def main():
    base_dir = os.path.dirname(__file__)
    path = os.path.join(base_dir, "links.h5")
    with h5py.File(path, "w") as f:
        grp = f.create_group("group")
        data = np.arange(4, dtype=np.int32)
        dset = grp.create_dataset("data", data=data)
        dset.attrs["attr"] = np.int32(7)

        # Hard link to the same dataset at root
        f["hard_link"] = dset

        # Soft link to the dataset
        f["soft_link"] = h5py.SoftLink("/group/data")

        # Soft link to a group (for h5_tree coverage)
        f["soft_group"] = h5py.SoftLink("/group")

    print("Created links.h5 successfully!")


if __name__ == "__main__":
    main()
