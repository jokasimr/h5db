#!/usr/bin/env python3
"""Create more than two vectors of run values for batched predicate tests."""

import h5py
import numpy as np


run_count = 4113
numbers = np.arange(run_count, dtype=np.int64)
text = np.array([f"{i:030d}".encode() for i in numbers])
lists = np.array([f"[{i}, NULL]".encode() for i in numbers])
bad_text = text.copy()
bad_lists = lists.copy()
# Both full batches contain a failure; the final partial batch is valid.
bad_text[[0, 2048]] = b"not-a-number"
bad_lists[[0, 2048]] = b"[bad, NULL]"

with h5py.File("predicate_pushdown_batches.h5", "w") as f:
    f["row_id"] = np.arange(3 * run_count, dtype=np.int64)
    f["rse_boundaries"] = (3 * numbers).astype(np.uint64)
    f["ree_boundaries"] = (3 * numbers + 2).astype(np.uint64)
    f["number"] = numbers
    f["cycle"] = (numbers % 5).astype(np.uint8)
    f["text"] = text
    f["lists"] = lists
    f["bad_text"] = bad_text
    f["bad_lists"] = bad_lists

print("Created predicate_pushdown_batches.h5")
