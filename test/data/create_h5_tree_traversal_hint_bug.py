import os
import h5py
import numpy as np


def main():
    base_dir = os.path.dirname(__file__)
    path = os.path.join(base_dir, "h5_tree_traversal_hint_bug.h5")
    with h5py.File(path, "w") as f:
        f.create_dataset("/foo/bar", data=np.array([1], dtype=np.int32))
        f.create_dataset("/foo/barbaz", data=np.array([2], dtype=np.int32))
        f.create_dataset("/foo/qux", data=np.array([3], dtype=np.int32))
        grp = f.create_group("/grp/sub")
        grp.create_dataset("leaf", data=np.array([4], dtype=np.int32))
        f.create_dataset("/grp/submarine", data=np.array([5], dtype=np.int32))

    print("Created h5_tree_traversal_hint_bug.h5 successfully!")


if __name__ == "__main__":
    main()
