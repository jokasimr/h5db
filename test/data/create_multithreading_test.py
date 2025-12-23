#!/usr/bin/env python3
"""Create HDF5 test file for multithreading tests.

This creates a small file with multiple similar datasets (simulating multiple
detectors/channels) that can be read in parallel and combined with UNION ALL.
This mimics the structure of large scientific files (like the bifrost file)
but with much smaller data for fast testing.

Structure:
  /detector_1/
    - time_offset (uint32, 1000 rows)
    - event_id (uint32, 1000 rows)
    - event_index (uint64, ~10 runs expanding to 1000 rows, RSE run_starts)
    - event_time_zero (uint64, ~10 values, RSE values)
  /detector_2/
    ... (same structure, different data)
  /detector_3/
    ... (same structure, different data)
  ... up to /detector_10/

This allows testing:
- UNION ALL with 2, 5, 10+ datasets
- Parallel RSE expansion
- Multiple file accesses in a single query
- All without requiring a 1.6GB test file
"""

import h5py
import numpy as np

NUM_DETECTORS = 10
ROWS_PER_DETECTOR = 1000
NUM_RUNS = 10  # Number of run-start entries (will expand to ROWS_PER_DETECTOR)


def create_detector_data(f, detector_id):
    """Create data for one detector (similar to bifrost channel structure)."""
    grp = f.create_group(f'detector_{detector_id}')

    # Regular columns
    # time_offset: increasing timestamps with some variation
    time_offset = np.arange(ROWS_PER_DETECTOR, dtype=np.uint32) * 1000 + detector_id * 100
    grp.create_dataset('time_offset', data=time_offset)

    # event_id: sequential IDs starting from detector offset
    event_id = np.arange(ROWS_PER_DETECTOR, dtype=np.uint32) + detector_id * 10000
    grp.create_dataset('event_id', data=event_id)

    # RSE columns (Run-Start Encoding)
    # event_index: run-starts indicating where each time_zero value begins
    # Divide rows evenly among runs
    rows_per_run = ROWS_PER_DETECTOR // NUM_RUNS
    event_index = np.array([i * rows_per_run for i in range(NUM_RUNS)], dtype=np.uint64)
    grp.create_dataset('event_index', data=event_index)

    # event_time_zero: the time values that get expanded via RSE
    # Each detector has slightly different time_zero values
    event_time_zero = np.array(
        [1000000000 + detector_id * 1000000 + i * 50000 for i in range(NUM_RUNS)], dtype=np.uint64
    )
    grp.create_dataset('event_time_zero', data=event_time_zero)

    return grp


with h5py.File('multithreading_test.h5', 'w') as f:
    print(f"Creating multithreading test file with {NUM_DETECTORS} detectors...")

    for detector_id in range(1, NUM_DETECTORS + 1):
        create_detector_data(f, detector_id)
        print(f"  Created detector_{detector_id}: {ROWS_PER_DETECTOR} rows, {NUM_RUNS} RSE runs")

    # Add metadata attributes for documentation
    f.attrs['description'] = 'Test file for multithreading and UNION ALL queries'
    f.attrs['num_detectors'] = NUM_DETECTORS
    f.attrs['rows_per_detector'] = ROWS_PER_DETECTOR
    f.attrs['num_rse_runs'] = NUM_RUNS
    f.attrs['created_by'] = 'create_multithreading_test.py'

    print(f"\nFile created successfully!")
    print(f"Total rows across all detectors: {NUM_DETECTORS * ROWS_PER_DETECTOR}")
    print(f"Structure: Each detector has 2 regular columns + RSE-encoded column")
