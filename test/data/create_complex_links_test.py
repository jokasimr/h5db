import os

import h5py
import numpy as np


def main():
    base_dir = os.path.dirname(__file__)
    path = os.path.join(base_dir, "complex_links.h5")

    with h5py.File(path, "w") as f:
        anchor = f.create_group("anchor")
        anchor.create_dataset("a_data", data=np.array([10, 20], dtype=np.int32))
        leaf = anchor.create_group("leaf")
        leaf.create_dataset("leaf_data", data=np.array([1, 2, 3], dtype=np.int16))
        leaf["up_to_anchor"] = h5py.SoftLink("/anchor")

        chain1 = f.create_group("chain1")
        chain1.create_dataset("c1_data", data=np.array([1], dtype=np.int8))

        chain2 = f.create_group("chain2")
        chain2.create_dataset("c2_data", data=np.array([2], dtype=np.int8))

        chain3 = f.create_group("chain3")
        chain3.create_dataset("c3_data", data=np.array([3], dtype=np.int8))

        messy = f.create_group("messy")

        # Hard-link aliases of the same anchor subtree.
        f["anchor_alias"] = anchor
        chain3["hard_anchor"] = anchor
        messy["hard_c2"] = chain2
        messy["hard_c3_data"] = chain3["c3_data"]

        # Soft-link chain that walks across the file and back into aliases.
        chain1["to_chain2"] = h5py.SoftLink("/chain2")
        chain2["to_anchor"] = h5py.SoftLink("/anchor")
        chain2["to_chain3"] = h5py.SoftLink("/chain3")
        chain3["to_leaf_data"] = h5py.SoftLink("/anchor/leaf/leaf_data")
        messy["soft_anchor_alias"] = h5py.SoftLink("/chain3/hard_anchor")

        # Dangling link to ensure unresolved-link behavior remains covered.
        messy["dangling"] = h5py.SoftLink("/missing")

    print("Created complex_links.h5 successfully!")


if __name__ == "__main__":
    main()
