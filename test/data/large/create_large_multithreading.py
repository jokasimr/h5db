#!/usr/bin/env python3
"""Create large-scale HDF5 test file for multithreading and parallelism testing.

This creates a file with multiple detectors, each having ~2M rows, totaling ~20M rows.
With DuckDB's parallelism threshold of 128K rows/thread, UNION ALL queries will
trigger extensive parallel execution, stressing the global HDF5 mutex.

Structure mirrors multithreading_test.h5 but scaled up.
"""

import h5py
import numpy as np

NUM_DETECTORS = 10
ROWS_PER_DETECTOR = 2_000_000  # 2M rows per detector = 20M total
NUM_RUNS = 200  # More runs for larger datasets (avg run length ~10K)

print(f"Creating large multithreading test file...")
print(f"  Detectors: {NUM_DETECTORS}")
print(f"  Rows per detector: {ROWS_PER_DETECTOR:,}")
print(f"  Total rows: {NUM_DETECTORS * ROWS_PER_DETECTOR:,}")
print(f"  RSE runs per detector: {NUM_RUNS}")


def create_detector_data(f, detector_id):
    """Create data for one detector (similar to bifrost channel structure)."""
    grp = f.create_group(f'detector_{detector_id}')

    # Regular columns
    # time_offset: increasing timestamps with detector-specific offset
    time_offset = np.arange(ROWS_PER_DETECTOR, dtype=np.uint32) * 1000 + detector_id * 100
    grp.create_dataset('time_offset', data=time_offset, chunks=(500_000,))

    # event_id: sequential IDs starting from detector offset
    event_id_offset = (detector_id - 1) * ROWS_PER_DETECTOR  # Start from 0 for detector_1
    event_id = np.arange(ROWS_PER_DETECTOR, dtype=np.uint32) + event_id_offset
    grp.create_dataset('event_id', data=event_id, chunks=(500_000,))

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

    print(f"  OK detector_{detector_id}: {ROWS_PER_DETECTOR:,} rows, {NUM_RUNS} RSE runs")


with h5py.File('large_multithreading.h5', 'w') as f:
    for detector_id in range(1, NUM_DETECTORS + 1):
        create_detector_data(f, detector_id)

    # Add metadata
    f.attrs['description'] = 'Large-scale test file for multithreading and parallel execution'
    f.attrs['num_detectors'] = NUM_DETECTORS
    f.attrs['rows_per_detector'] = ROWS_PER_DETECTOR
    f.attrs['num_rse_runs'] = NUM_RUNS
    f.attrs['created_by'] = 'create_large_multithreading.py'
    f.attrs['total_rows'] = NUM_DETECTORS * ROWS_PER_DETECTOR

print(f"\nOK File created successfully!")
print(f"Filename: large_multithreading.h5")
print(f"Total size: Each detector ~{ROWS_PER_DETECTOR * 8 / 1024 / 1024:.0f}MB for uint32 columns")
