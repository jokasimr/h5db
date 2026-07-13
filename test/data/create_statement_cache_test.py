#!/usr/bin/env python3
"""Create compressed fixtures used to verify prepared-statement rebinding."""

import gzip
from pathlib import Path

import h5py
import numpy as np


def compress_fixture(filename: str) -> None:
    path = Path(filename)
    path.with_suffix(path.suffix + ".gz").write_bytes(gzip.compress(path.read_bytes(), mtime=0))
    path.unlink()


with h5py.File("statement_cache_initial.h5", "w") as f:
    f.create_dataset("x", data=np.array([1, 2, 3], dtype=np.int32))
    f.create_dataset("s", data=np.int32(7))
    f.attrs["a"] = np.int32(7)
compress_fixture("statement_cache_initial.h5")

with h5py.File("statement_cache_replacement.h5", "w") as f:
    f.create_dataset(
        "x",
        data=np.array([10.5, 20.5, 30.5, 40.5, 50.5], dtype=np.float64),
    )
    f.create_dataset("s", data=np.float64(10.5))
    f.attrs["a"] = np.float32(10.5)
compress_fixture("statement_cache_replacement.h5")

print("Created compressed statement-cache fixtures successfully!")
