import os

import h5py
import numpy as np


def create_file(filename: str, enable_swmr: bool) -> None:
    # Use latest format when enabling SWMR to satisfy HDF5 requirements
    libver = "latest" if enable_swmr else None
    kwargs = {"libver": libver} if libver else {}
    with h5py.File(filename, "w", **kwargs) as f:
        data = np.arange(5, dtype=np.int32)
        dset = f.create_dataset("data", data=data)
        dset.attrs["attr"] = np.int32(42)
        f.flush()

        if enable_swmr:
            # Switch to SWMR write mode; this marks the superblock while the file is open
            f.swmr_mode = True
            f.flush()


def main() -> None:
    base_dir = os.path.dirname(__file__)
    create_file(os.path.join(base_dir, "swmr_enabled.h5"), True)
    create_file(os.path.join(base_dir, "swmr_disabled.h5"), False)
    print("Created swmr_enabled.h5 and swmr_disabled.h5")


if __name__ == "__main__":
    main()
