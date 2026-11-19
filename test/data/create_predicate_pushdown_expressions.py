#!/usr/bin/env python3
"""Run expressions spanning batches, NULL coverage, and a late conversion error."""

import h5py
import numpy as np


# Two full 2048-value batches and a partial batch.
numbers = np.arange(4113, dtype=np.int64)
epoch = np.datetime64("2026-05-12T00:00:00", "us").astype(np.int64)
for suffix in ("clean", "bad"):
    with h5py.File(f"predicate_pushdown_expressions_{suffix}.h5", "w") as f:
        f["row_id"] = np.arange(3 * (len(numbers) + 1), dtype=np.int64)
        # Three leading NULL rows for RSE; three trailing NULL rows for REE.
        f["rse_boundaries"] = (3 * numbers + 3).astype(np.uint64)
        f["ree_boundaries"] = (3 * numbers + 2).astype(np.uint64)
        f["number"] = numbers
        f["ts"] = epoch + numbers * 1_000_000
        f["label"] = np.array([f"sample-{i}".encode() for i in numbers])
        text = np.array([f"{i:030d}".encode() for i in numbers])
        if suffix == "bad":
            text[4000] = b"not-a-number"
        f["text"] = text
        f["empty_boundaries"] = np.array([], dtype=np.uint64)
        f["empty_values"] = np.array([], dtype=np.int64)
        # Uneven runs, duplicate boundaries, and NULL coverage for range intersections.
        f["coarse_rse_boundaries"] = np.array([3, 3, 30, 9000, 9000, 12339, 12342], dtype=np.uint64)
        f["coarse_ree_boundaries"] = np.array([2, 2, 29, 8999, 8999, 12338, 12338], dtype=np.uint64)
        f["coarse"] = np.array([99, 10, 20, 99, 30, 40, 99], dtype=np.int64)
        f["scalar"] = np.int64(42)
    print(f"Created predicate_pushdown_expressions_{suffix}.h5")
