# Run-Start Encoding (RSE) Support in h5db

## Overview

Run-start encoding (RSE), also known as run-length encoding (RLE), is a compression technique that efficiently stores repeated values by recording where values change rather than storing every individual value. This is particularly valuable for datasets with long sequences of identical values.

**Example**: Instead of storing `[100, 100, 100, 200, 200, 200, 200, 300, 300, 300]` (10 values), RSE stores:
- `run_starts`: `[0, 3, 7]` (3 values - where runs begin)
- `values`: `[100, 200, 300]` (3 values - what the values are)

This achieves a **3.3× compression ratio** in this simple example. Real-world datasets often achieve 100×-1000× compression.

## When to Use RSE

RSE is ideal for:
- **State variables** that change infrequently (e.g., device status, experiment phases)
- **Categorical data** with long runs (e.g., quality flags, operational modes)
- **Step functions** in time-series data
- **Segmented data** where values remain constant within segments

## API Usage

### The `h5_rse()` Function

The `h5_rse()` scalar function creates a specification for a run-start encoded column:

```sql
h5_rse(run_starts_path, values_path) → STRUCT
```

**Parameters**:
- `run_starts_path`: Path to the dataset containing run start indices (must be integer type)
- `values_path`: Path to the dataset containing the values for each run

**Returns**: A struct with `{encoding, run_starts, values}` fields used internally by `h5_read()`

### Reading RSE Data with `h5_read()`

Use `h5_rse()` within `h5_read()` to specify which columns are run-encoded:

```sql
SELECT * FROM h5_read(
    'file.h5',
    '/regular/column',                           -- Regular dataset
    h5_rse('/rse/run_starts', '/rse/values'),   -- RSE column
    '/another/regular/column'                    -- Another regular dataset
);
```

**Important**: At least one non-scalar regular (non-RSE) column is required to determine the total row count.

## Complete Examples

### Example 1: Basic Usage

```sql
-- Load the extension
LOAD 'h5db.duckdb_extension';

-- Read a dataset with one regular and one RSE column
SELECT * FROM h5_read(
    'experiment.h5',
    '/data/timestamp',                                    -- Regular column (10 rows)
    h5_rse('/data/state_run_starts', '/data/state_values') -- RSE column
);
```

**Output**:
```
┌───────────┬──────────────┐
│ timestamp │ state_values │
│   int64   │    int32     │
├───────────┼──────────────┤
│         0 │          100 │
│         1 │          100 │
│         2 │          100 │
│         3 │          200 │
│         4 │          200 │
│         5 │          200 │
│         6 │          200 │
│         7 │          300 │
│         8 │          300 │
│         9 │          300 │
└───────────┴──────────────┘
```

### Example 2: Aggregation

RSE columns work seamlessly with SQL aggregations:

```sql
-- Count rows by state
SELECT state_values, COUNT(*) as count
FROM h5_read(
    'experiment.h5',
    '/data/timestamp',
    h5_rse('/data/state_run_starts', '/data/state_values')
)
GROUP BY state_values
ORDER BY state_values;
```

**Output**:
```
┌──────────────┬───────┐
│ state_values │ count │
├──────────────┼───────┤
│          100 │     3 │
│          200 │     4 │
│          300 │     3 │
└──────────────┴───────┘
```

### Example 3: Filtering

Filter on RSE columns just like regular columns:

```sql
-- Get only rows where state is 200
SELECT timestamp, state_values
FROM h5_read(
    'experiment.h5',
    '/data/timestamp',
    h5_rse('/data/state_run_starts', '/data/state_values')
)
WHERE state_values = 200;
```

### Example 4: Multiple RSE Columns

You can mix multiple regular and RSE columns:

```sql
SELECT * FROM h5_read(
    'sensor_data.h5',
    '/sensors/timestamp',                                      -- Regular
    '/sensors/measurement',                                    -- Regular
    h5_rse('/sensors/status_starts', '/sensors/status_vals'), -- RSE
    h5_rse('/sensors/mode_starts', '/sensors/mode_vals')      -- RSE
)
WHERE status_vals = 'active';
```

### Example 5: High Compression Scenario

Real-world example with 1000 rows compressed from 4 runs (250× compression):

```sql
-- Compute statistics per status
SELECT
    status_values,
    COUNT(*) as n_measurements,
    AVG(measurement) as avg_value,
    MIN(measurement) as min_value,
    MAX(measurement) as max_value
FROM h5_read(
    'large_experiment.h5',
    '/data/measurement',
    h5_rse('/data/status_run_starts', '/data/status_values')
)
GROUP BY status_values;
```

## HDF5 File Structure

### Required Structure

For RSE to work, your HDF5 file must contain:

1. **At least one regular dataset** (defines the total row count)
2. **For each RSE column**: Two datasets:
   - `<name>_run_starts`: Integer array of indices where values change
   - `<name>_values`: Array of values for each run

### Example File Structure

```
/experiment/
├── timestamp          [1000 elements]  ← Regular dataset (defines row count)
├── measurement        [1000 elements]  ← Regular dataset
├── status_run_starts  [4 elements]     ← RSE indices: [0, 200, 500, 900]
└── status_values      [4 elements]     ← RSE values: [0, 1, 0, 2]
```

In this example:
- Rows 0-199: status = 0
- Rows 200-499: status = 1
- Rows 500-899: status = 0
- Rows 900-999: status = 2

### Creating RSE Data in Python

```python
import h5py
import numpy as np

with h5py.File('experiment.h5', 'w') as f:
    # Regular dataset (10 rows - this defines the total length)
    f.create_dataset('timestamp', data=np.arange(10))

    # RSE column: logical data is [100, 100, 100, 200, 200, 200, 200, 300, 300, 300]
    f.create_dataset('state_run_starts', data=np.array([0, 3, 7]))
    f.create_dataset('state_values', data=np.array([100, 200, 300]))
```

## Run-Start Semantics

### Indexing Rules

- **Strictly increasing**: Each value must be greater than the previous
- **Leading NULLs**: If `run_starts[0] > 0`, rows before the first run start are returned as NULLs
- **Implicit end**: The last run extends to the end of the dataset

### Example

Given:
- `run_starts = [0, 3, 7]`
- `values = [100, 200, 300]`
- Total rows = 10

Expansion:
- Run 0: rows 0-2 (indices 0, 1, 2) → value 100
- Run 1: rows 3-6 (indices 3, 4, 5, 6) → value 200
- Run 2: rows 7-9 (indices 7, 8, 9) → value 300

## Supported Data Types

### run_starts Types
- `int32` (signed 32-bit integer)
- `int64` (signed 64-bit integer)
- `uint32` (unsigned 32-bit integer)
- `uint64` (unsigned 64-bit integer)

### values Types
All types supported by h5db:
- **Integer**: int8, int16, int32, int64
- **Unsigned**: uint8, uint16, uint32, uint64
- **Float**: float32, float64
- **String**: fixed-length and variable-length

## Performance Characteristics

### Memory Usage

RSE data is **expanded in memory** during the initialization phase:
- **run_starts** and **values** are read into memory (typically small)
- During scanning, values are emitted on-the-fly without creating a full expanded array
- Memory usage: O(num_runs) not O(num_rows)

### Compression Ratios

Typical compression ratios by use case:
- **State variables**: 100×-1000× (e.g., 10 states over 100,000 rows)
- **Quality flags**: 50×-500× (e.g., occasional flag changes)
- **Operational modes**: 10×-100× (e.g., switching between 3-5 modes)

### Query Performance

- **Filtering** (`WHERE status = X`): Fast - DuckDB can process the expanded values efficiently
- **Aggregation** (`GROUP BY status`): Fast - benefits from data locality
- **Sequential scan**: O(n) where n is the number of output rows
- **Expansion overhead**: Single-pass O(1) amortized per row

### I/O Benefits

For remote files (S3, HTTP):
- Only need to read small run_starts and values datasets
- Significant bandwidth savings (100×-1000× less data transferred)

## Validation and Error Handling

### Automatic Validation

h5db automatically validates:
1. ✅ `run_starts` is strictly increasing
2. ✅ `run_starts.size() == values.size()`
3. ✅ At least one regular column exists

### Error Messages

**Error: No regular columns**
```
IO Error: h5_read requires at least one regular (non-RSE) dataset to determine row count
```
**Solution**: Include at least one regular dataset path in `h5_read()`

**Error: Invalid run_starts**
```
IO Error: RSE run_starts must be strictly increasing
```
**Solution**: Ensure `run_starts` is strictly increasing

**Error: Size mismatch**
```
IO Error: RSE run_starts and values must have same size. Got 3 and 4
```
**Solution**: Ensure run_starts and values arrays have the same length

## Best Practices

### 1. Choose RSE Wisely
✅ **Good candidates**:
- Variables that change infrequently
- Categorical data with long runs
- Step functions

❌ **Poor candidates**:
- Rapidly changing values
- Unique values at every row
- Continuous measurements

### 2. Naming Convention
Use consistent naming for RSE datasets:
```
<column_name>_run_starts
<column_name>_values
```

This makes the relationship clear when inspecting the HDF5 file.

### 3. Include Regular Columns
Always include at least one regular "index" column:
```sql
-- Good: timestamp provides the index
h5_read('file.h5', '/timestamp', h5_rse('/state_starts', '/state_vals'))

-- Bad: RSE only (will error)
h5_read('file.h5', h5_rse('/state_starts', '/state_vals'))
```

### 4. Verify Compression Benefit
Before using RSE, check if you actually get compression:
```python
compression_ratio = total_rows / num_runs
# If ratio < 2, probably not worth using RSE
```

### 5. Document Your Schema
Add HDF5 attributes to document RSE columns:
```python
f['state_run_starts'].attrs['description'] = 'Run starts for state column'
f['state_run_starts'].attrs['values_dataset'] = 'state_values'
```

## Troubleshooting

### Problem: Unexpected row count

**Symptom**: Query returns fewer rows than expected

**Cause**: Row count is determined by the shortest regular dataset

**Solution**: Ensure all regular datasets have the same length as intended

### Problem: Values don't expand correctly

**Symptom**: Wrong values in output

**Cause**: run_starts isn't strictly increasing

**Solution**: Validate run_starts array:
```python
assert all(run_starts[i] < run_starts[i+1] for i in range(len(run_starts)-1))
```

### Problem: Out of memory

**Symptom**: Process crashes during query

**Cause**: Extremely large expanded dataset

**Solution**: Consider filtering before expansion or processing in chunks

## Comparison with Alternatives

### vs. Full Expansion in HDF5
**RSE Pros**:
- Much smaller file size (100×-1000× smaller)
- Faster file I/O
- Better for remote files

**Full Expansion Pros**:
- Simpler to implement in writing code
- No validation needed

### vs. Separate Index Table
**RSE Pros**:
- Single file contains all data
- Automatic expansion during query
- No JOIN needed

**Index Table Pros**:
- More flexible for complex patterns
- Can use standard SQL tools

## Limitations

1. **At least one regular column required** - Cannot query RSE columns alone
2. **No random access optimization** - Each query scans from the beginning
3. **Memory overhead** - run_starts and values loaded into memory
4. **No compression in output** - DuckDB receives fully expanded data

## Future Enhancements

Potential future improvements:
- Automatic RSE detection based on naming conventions
- Support for RSE-only queries (auto-generate index)
- Integration with DuckDB's internal compression
- Lazy evaluation for filtered queries

## Examples Repository

See `test/data/run_encoded.h5` for complete working examples:
- Basic int32 expansion
- High compression ratio (1000 rows → 4 runs)
- String value support
- Edge cases

## See Also

- [README.md](README.md) - Main h5db documentation
- [API.md](API.md) - Complete API reference
- [docs/DEVELOPER.md](docs/DEVELOPER.md) - Developer guide
