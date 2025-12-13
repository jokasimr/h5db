# Run-Length Encoding Support Investigation

## Problem Statement

HDF5 files often contain run-length encoded datasets to save space when values are repeated.

**Key insight**: Groups contain:
1. One or more **regular "long" datasets** (A) that define the row count
2. Zero or more **run-encoded datasets** (B) represented by pairs:
   - `B_run_starts`: Indices where values change (e.g., `[0, 3, 7]`)
   - `B_values`: Corresponding values (e.g., `[100, 200, 300]`)

**IMPORTANT**: The total row count is determined by the regular dataset(s), NOT by the run-encoded data.

**Example**:
```
Group /experiment:
  - timestamp: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]  (10 rows - defines length)
  - state_run_starts: [0, 3, 7]
  - state_values: [100, 200, 300]

Desired table (10 rows):
┌───────────┬───────┐
│ timestamp │ state │
│   int64   │ int32 │
├───────────┼───────┤
│         0 │   100 │  ← run 0: indices 0-2
│         1 │   100 │
│         2 │   100 │
│         3 │   200 │  ← run 1: indices 3-6
│         4 │   200 │
│         5 │   200 │
│         6 │   200 │
│         7 │   300 │  ← run 2: indices 7-9
│         8 │   300 │
│         9 │   300 │
└───────────┴───────┘

State is decoded from run_starts/values to match timestamp's length.
```

**Goal**: Read entire groups where some columns are regular and some are run-encoded, producing a normalized table.

## Questions to Investigate

### 1. HDF5 Conventions

**Questions**:
- Is there a standard HDF5 convention for run-length encoding?
- How are `run_starts` defined?
  - 0-indexed or 1-indexed?
  - Inclusive or exclusive ranges?
  - What about the last run (implicit end at dataset length)?
- Are there HDF5 attributes that indicate a dataset is run-encoded?
- What naming conventions are used?
  - `{name}_run_starts` and `{name}_values`?
  - `{name}_indices` and `{name}_data`?
  - Other patterns?

**Investigation needed**:
- Check real HDF5 files for examples
- Look for documentation on HDF5 run-length encoding standards
- Examine attributes on run-encoded datasets

### 2. DuckDB Run-End Encoding Capabilities

**Questions**:
- Does DuckDB have a run-end encoded (REE) physical type?
- Can we create columns with RLE storage format?
- What are the performance characteristics?
  - Query performance on RLE columns
  - Memory usage
  - Compression ratios

**Investigation needed**:
- Search DuckDB documentation for RLE/REE support
- Check if DuckDB has `LIST` or `STRUCT` types that could represent runs
- Test performance of expanded vs compressed representations

**Initial research**:
```sql
-- DuckDB might have compression, but is it exposed at the logical type level?
-- Need to check:
-- 1. COMPRESSION pragma/settings
-- 2. Physical storage formats
-- 3. Whether RLE is automatic or configurable
```

### 3. API Design Options

Given that we're reading entire **groups** with mixed regular/run-encoded datasets:

#### Option 1: Dedicated Function with Convention-Based Detection

```sql
-- Read entire group, auto-detect and expand run-encoded datasets
SELECT * FROM h5_read_group('file.h5', '/experiment');

-- Automatically:
-- 1. Finds regular datasets (timestamp, measurement, etc.)
-- 2. Detects pairs like state_run_starts + state_values
-- 3. Expands run-encoded datasets to match regular dataset length
-- 4. Returns table with columns: timestamp, measurement, state, ...
```

**Detection logic**:
- If datasets `X_run_starts` and `X_values` exist → decode to column `X`
- Otherwise, read dataset `X` normally

**Pros**:
- Clean, simple API
- Works with standard naming convention
- User doesn't specify encoding details

**Cons**:
- Relies on naming convention (`_run_starts`, `_values`)
- What if naming differs?
- Silently fails if convention not matched

#### Option 2: Explicit Mapping Function

```sql
-- Specify which datasets are run-encoded
SELECT * FROM h5_read_group('file.h5', '/group', {
  'A': 'normal',
  'B': 'run_encoded'
});

-- Or more explicit:
SELECT * FROM h5_read_run_decoded(
  'file.h5',
  '/group/A',  -- normal dataset
  run_encode('/group/B_run_starts', '/group/B_values', 'B')  -- decoded as column B
);
```

**Pros**:
- Explicit control
- No ambiguity
- Can mix encoded/non-encoded

**Cons**:
- More verbose
- User must know encoding structure

#### Option 3: Convention-Based Auto-Detection

```sql
-- Convention: if B_run_starts and B_values exist, decode B
SELECT * FROM h5_read_group('file.h5', '/group', ['A', 'B']);

-- Looks for:
--   /group/A (read directly)
--   /group/B_run_starts + /group/B_values (decode to B)
--   OR /group/B (read directly if no run_starts/values)
```

**Pros**:
- Best of both worlds
- Fallback to normal reading
- Works with standard naming

**Cons**:
- Relies on naming convention
- Less flexible for non-standard naming

#### Option 4: Separate Utility Function

```sql
-- First read the run-encoded datasets
WITH run_data AS (
  SELECT unnest(B_run_starts) as start_idx,
         unnest(B_values) as value
  FROM h5_read('file.h5', '/group/B_run_starts', '/group/B_values')
)
-- Then expand (user-defined logic)
...
```

**Pros**:
- Uses existing h5_read()
- Flexible
- No new API surface

**Cons**:
- User must implement expansion logic
- Not ergonomic
- Defeats purpose of convenience

### 4. Implementation Approaches

#### Approach A: Expand During Bind Phase

```cpp
// In H5ReadBind:
// 1. Detect run-encoded pattern
// 2. Calculate expanded size
// 3. Set num_rows to expanded size
// 4. Store run_starts and values in bind_data
```

**Pros**:
- Schema is known upfront
- Simple scan logic

**Cons**:
- Must read metadata during bind
- Can't easily stream large datasets

#### Approach B: Expand During Scan Phase

```cpp
// In H5ReadScan:
// 1. Track current position in expanded space
// 2. Map position to (run_index, offset_in_run)
// 3. Emit appropriate value
// 4. Handle chunk boundaries
```

**Pros**:
- Streaming-friendly
- Less memory if DuckDB can compress

**Cons**:
- More complex scan logic
- Coordinate multiple read positions

#### Approach C: Expand to Intermediate Buffer

```cpp
// In H5ReadInit:
// 1. Read entire run_starts and values
// 2. Expand to full array in memory
// 3. Scan reads from expanded buffer
```

**Pros**:
- Simple scan logic
- Reuses existing read code

**Cons**:
- High memory usage
- Not suitable for huge datasets
- Negates benefit of RLE

#### Approach D: DuckDB-Native RLE (if available)

```cpp
// If DuckDB supports RLE types:
// 1. Read run_starts and values
// 2. Create RLE-typed column directly
// 3. DuckDB handles expansion on query
```

**Pros**:
- Best performance
- Minimal memory
- Leverages DuckDB internals

**Cons**:
- Only works if DuckDB supports it
- Need to understand DuckDB RLE format

### 5. Efficiency Considerations

**Storage efficiency**:
- RLE format: `O(num_runs)` storage
- Expanded format: `O(num_elements)` storage
- For highly repetitive data: 100x-1000x compression possible

**Query performance**:
- **Filters**: If `WHERE B = 200`, can skip entire runs
- **Aggregates**: Can compute `COUNT(*)` from run lengths
- **Point lookups**: Need binary search in run_starts
- **Sequential scan**: Must expand all values

**Memory considerations**:
- Large datasets (10M+ elements) with few runs (100s):
  - RLE: KBs
  - Expanded: MBs-GBs
- But DuckDB query engine expects materialized columns (usually)

**Network/Disk I/O**:
- Read run_starts + values: Small I/O
- Expansion happens in memory
- For remote files (S3, HTTP): RLE is much better

### 6. Run-Start Semantics to Clarify

**Need to determine**:
1. **Indexing**: 0-based or 1-based?
2. **Ranges**:
   - Run at index `i` goes from `run_starts[i]` to `run_starts[i+1]-1`?
   - Or from `run_starts[i]` with length `run_lengths[i]`?
   - Last run goes to end of dataset?
3. **Validation**:
   - Must `run_starts[0] == 0`?
   - Must `run_starts` be sorted?
   - Can there be gaps?

**Example to test**:
```python
# Create test HDF5 with run-encoded dataset
import h5py
import numpy as np

with h5py.File('test_run_encoded.h5', 'w') as f:
    # Regular dataset
    f.create_dataset('A', data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # Run-encoded dataset B
    # Logical: [100, 100, 100, 200, 200, 200, 200, 300, 300, 300]
    f.create_dataset('B_run_starts', data=[0, 3, 7])
    f.create_dataset('B_values', data=[100, 200, 300])

    # Add attributes to indicate encoding?
    f['B_run_starts'].attrs['encoding'] = 'run_starts'
    f['B_run_starts'].attrs['values_dataset'] = 'B_values'
    f['B_run_starts'].attrs['logical_length'] = 10
```

## Recommended Investigation Steps

### Step 1: Research & Documentation (1-2 hours)

1. **Search for HDF5 run-encoding standards**:
   - HDF5 official documentation
   - Scientific computing libraries (h5py, NetCDF, etc.)
   - Domain-specific formats (astronomy, genomics, climate)

2. **Check DuckDB RLE support**:
   - Read DuckDB storage documentation
   - Check for `RLE`, `REE`, or `RUN_ENCODED` in DuckDB source
   - Test if compression happens automatically

3. **Look at real examples**:
   - Ask user for sample file
   - Examine structure and conventions

### Step 2: Create Test Data (30 mins)

```bash
# Create Python script to generate test HDF5 file
python3 scripts/create_run_encoded_test.py
```

Test cases:
- Simple run-encoded dataset
- Mixed group (normal + run-encoded)
- Different data types (int, float, string)
- Edge cases (single run, all different values)

### Step 3: Prototype Expansion Logic (1-2 hours)

```cpp
// Standalone function to test expansion
std::vector<T> ExpandRunEncoded(
    const std::vector<idx_t>& run_starts,
    const std::vector<T>& values,
    idx_t total_length
) {
    std::vector<T> result(total_length);
    for (size_t i = 0; i < run_starts.size(); i++) {
        idx_t start = run_starts[i];
        idx_t end = (i + 1 < run_starts.size())
                    ? run_starts[i + 1]
                    : total_length;
        std::fill(result.begin() + start, result.begin() + end, values[i]);
    }
    return result;
}
```

### Step 4: Benchmark Approaches (1 hour)

Compare:
- Full expansion vs RLE storage
- Memory usage
- Query performance on different operations

### Step 5: Design API (1 hour)

Based on findings, choose API approach and document:
- Function signature
- Parameter meanings
- Examples
- Error handling

### Step 6: Implementation Plan (30 mins)

Create detailed implementation plan:
- Data structures needed
- Bind/Init/Scan phase changes
- Testing strategy

## Open Questions for User

1. **Do you have example HDF5 files** with run-encoded datasets we can examine?
2. **What are the naming conventions** in your domain?
   - `{name}_run_starts` + `{name}_values`?
   - Something else?
3. **What is the typical compression ratio**?
   - How many elements vs how many runs?
   - Should we optimize for high compression (few runs) or low compression?
4. **Are there HDF5 attributes** that mark datasets as run-encoded?
5. **Should this be automatic or explicit**?
   - Auto-detect based on naming?
   - Require user to specify encoding?
6. **Priority: Memory vs Simplicity**?
   - OK to expand everything in memory (simpler)?
   - Must stay compressed (complex but efficient)?

## Next Steps

After investigation, we should:

1. ✅ Document findings in this file
2. ✅ Create test HDF5 files with run-encoded data
3. ✅ Choose API design
4. ✅ Create implementation plan
5. ✅ Start with simple prototype
6. ✅ Benchmark and optimize

---

**Status**: Investigation phase - awaiting user feedback on questions above
