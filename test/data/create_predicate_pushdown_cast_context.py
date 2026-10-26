#!/usr/bin/env python3
"""Create run-encoded strings for session-dependent cast pushdown tests."""

import h5py
import numpy as np


with h5py.File('predicate_pushdown_cast_context.h5', 'w') as f:
    f['row_id'] = np.arange(6, dtype=np.int64)
    f['rse_boundaries'] = np.array([0, 3], dtype=np.uint64)
    f['ree_boundaries'] = np.array([2, 5], dtype=np.uint64)
    f['stamp'] = np.array([b'2026-01-01 12:00:00', b'2026-01-01 13:00:00'])
    f['bad_stamp'] = np.array([b'not-a-timestamp', b'2026-01-01 12:00:00'])

print('Created predicate_pushdown_cast_context.h5')
