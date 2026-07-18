import os

import h5py


def main():
    base_dir = os.path.dirname(__file__)
    path = os.path.join(base_dir, "h5_tree_vector_boundary.h5")

    # Including the root row, 4,095 links fill exactly two DuckDB vectors.
    # Use the modern dense-group representation expected for current files.
    with h5py.File(path, "w", libver="latest") as file:
        for index in range(4_095):
            file[f"link_{index:04d}"] = h5py.SoftLink("/missing")

    print("Created h5_tree_vector_boundary.h5 successfully!")


if __name__ == "__main__":
    main()
