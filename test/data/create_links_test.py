import os
import h5py
import numpy as np


def main():
    base_dir = os.path.dirname(__file__)
    target_path = os.path.join(base_dir, "links_external_target.h5")
    with h5py.File(target_path, "w") as target:
        ext_grp = target.create_group("external_group")
        ext_dset = ext_grp.create_dataset("external_data", data=np.arange(3, dtype=np.int16))
        ext_dset.attrs["attr"] = np.int32(11)

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

        # Dangling soft link for reader behavior checks
        f["dangling_link"] = h5py.SoftLink("/missing_dataset")

        # External link for reader behavior checks
        f["external_link"] = h5py.ExternalLink("links_external_target.h5", "/external_group/external_data")

    print("Created links_external_target.h5 and links.h5 successfully!")


if __name__ == "__main__":
    main()
