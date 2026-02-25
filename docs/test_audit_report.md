# H5DB Test Audit Report

This report verifies that expected outputs in each SQLLogicTest match the correct results implied by the data generators and HDF5 specs. I used the generator scripts in `test/data/` and `test/data/large/` as the source of truth. Where outputs depend on HDF5 iteration order (attributes), I validated the ordering against HDF5’s native attribute iteration and noted fragility.

## Summary of findings
- No mismatches found in numeric results derived directly from generators.
- `h5_attributes` output order matches HDF5 native attribute iteration for the generated file, but the order is not obviously lexicographic or insertion order; this test is correct for HDF5’s native order and is potentially fragile across HDF5 versions.

---

## test/sql/h5db.test
Data sources: `test/data/create_simple_types_multidim.py`, `test/data/create_run_encoded_test.py`, `test/data/create_attrs_test.py`.

### Extension identity/version
- `h5db_version('test')` → `ILIKE` checks only; correct.

### h5_tree() counts/types
- simple.h5: 10 objects total, 3 groups (group1, subgroup, root), 7 datasets, 2 int32 datasets (integers, matrix). Matches generator.
- simple.h5 dtype/shape checks for `/integers`, `/floats`, `/strings`, `/matrix`: match generator types and shapes.
- types.h5 dtype counts: 4 signed int, 3 unsigned int, 2 float datasets. Matches generator.
- multidim.h5 shapes for array_1d/2d/3d/4d: match generator shapes.
- non-existent file error: expected.

### h5_read() basics (simple.h5/types.h5/multidim.h5)
Each expected result matches generator arrays:
- `/integers` count 10, sum 45 (0..9).
- `/types.h5` integer/unsigned/float counts and sums match literal arrays.
- String datasets (`/strings`, `/var_strings`, `/fixed_strings`) match literal data and orderings.
- Nested groups: `/group1/data1` count 5 sum 10.0; `/group1/data2` count 10; `/group1/subgroup/nested_data` count 100.
- 2D matrix shape and element access: first row `[0,1,2,3]` → `matrix[1]=0`, `matrix[2]=1`. Sum of 0..19 = 190.
- 3D array access: first row contains 0..11 in row-major order → `array_3d[1][1]=0`, `array_3d[1][2]=1`, `array_3d[2][1]=3`. Count 5 rows.
- 4D array access: first elements `0` and `1` as expected.
- Missing file/dataset errors: expected.

### Column naming and type detection
Derived from dataset names and types:
- Column names: last path component (`integers`, `data1`, `nested_data`).
- Types: int32→INTEGER, float64→DOUBLE, string→VARCHAR, int8→TINYINT, uint8→UTINYINT, float32→FLOAT.
- Array types: matrix→INTEGER[4], array_3d→BIGINT[3][4], array_4d→BIGINT[2][3][4]. Matches BuildArrayType.

### Edge cases and integration
- array_1d count 10 sum 45: generator uses `np.arange(10)`.
- root path `/.'` appears in h5_tree: H5Ovisit includes root name `.`.
- Multi-dataset read uses min row count (strings length 3) → count 3, sum 3 for integers.
- Mixed datasets (group1/data1 + data2): first row `0.0` and `1`.

### h5_rse / h5_alias / h5_index
From `create_run_encoded_test.py`:
- RSE expansion for experiment1 matches run_starts [0,3,7], values [100,200,300].
- Count where state < 200.5 is 7 (100s + 200s).
- Experiment2 counts: 0→600, 1→300, 2→100.
- Experiment3 strings: `"low"` 4, `"high"` 4.
- Edge cases: single run distinct count 1; no_compression distinct count 5.
- h5_alias renames columns; h5_index produces 0..9 and filter `<1.2` yields two rows.

### h5_attributes (with_attrs.h5)
Generator defines scalar and array attributes on `/dataset_with_attrs`.
HDF5 native attribute iteration order (as returned by H5Aiterate2 with H5_INDEX_NAME/H5_ITER_NATIVE) in this file is:
`int8_attr, int16_attr, int_array_attr, float_array_attr, int32_attr, int64_attr, uint8_attr, uint16_attr, uint32_attr, uint64_attr, float32_attr, float64_attr, string_attr`.
Expected output matches this order and the literal values from `create_attrs_test.py`.
- Group attributes `/group_with_attrs`: 999, "I am a group", [10,20,30].
- Column selection queries match attribute values.
- Missing file/path errors: expected.

---

## test/sql/projection_pushdown.test
Data sources: simple.h5/types.h5.

All expected outputs align with the arrays in `create_simple_types_multidim.py` and `create_simple_types_multidim.py` types:
- Projections reflect subset of columns; ordering and filtering match literal arrays.
- Aggregations (COUNT, SUM, AVG) match known sums/means.
- Aliased columns behave as standard DuckDB aliases.
- Multi-type projections (int8/int16/uint8/float32) match literal data.
- Array projections: `typeof(matrix)` = INTEGER[4], count 5 rows in matrix dataset.
- ORDER BY non-projected column: integer ordering matches floats ordering in simple.h5.
- DESCRIBE output contains exactly projected columns.
- Baseline vs projected comparisons use EXCEPT; expected 0 mismatch.

---

## test/sql/predicate_pushdown.test
Data source: `test/data/create_pushdown_test.py`.

Generator summary:
- 1000 rows, run_starts [0,200,400,600,800].
- Sorted int RSE values [10,20,30,40,50], 200 rows each.
- Unsorted int RSE values [50,10,30,20,40].
- Sorted float RSE values [1.5,2.5,3.5,4.5,5.5].
- Sorted int64 RSE values [100,200,300,400,500].
- regular = 0..999, regular_float = 0.0..99.9.

All COUNT/MIN/MAX results follow from the 200-row runs and regular index ranges:
- `int_rse_values > 10` → rows 200..999 (800 rows), min regular 200.
- `>=20` same as above; `<20` gives 0..199 (200 rows), max 199.
- `<=20` gives 0..399 (400 rows), max 399.
- `=30` gives 400..599 (200 rows), min 400 max 599.
- BETWEEN [20,30] gives 200..599 (400 rows).
- BETWEEN [40,40] gives 200 rows (value 40 run).
- `>25 AND <35` matches only value 30 run (200 rows), min 400.
- First and last run filters return expected min/max.
- Multiple filters on same column intersect ranges; counts/min/max match intersection math in comments.
- Float and int64 RSE tests: ranges derived from runs aligned with 200-row blocks.
- Unsorted RSE tests: counts derived from unsorted values with same run boundaries.
- Mixed regular/RSE filters: counts derived from applying RSE range then post-filtering `regular` (e.g., `regular > 450` in 400..599 run yields 149 rows).
- NOT/OR tests: no optimization implied, but correctness counts align with union/exclusion of run blocks.
- Defensive filtering tests check distinct values and range bounds consistent with runs.

Each expected count/min/max in the file matches the run boundaries in the generator and the stated comments in the test.

---

## test/sql/rse_edge_cases.test
Data source: `test/data/create_rse_edge_cases.py`.

All expected outputs match the run definitions in the generator:
- Single-row dataset returns (0,42), count 1.
- Single-entry runs: 10 rows, 10 distinct values, sum 100+...+1000=5500.
- Chunk-aligned runs: 2048/2048/4 distribution; boundary checks and counts match.
- Constant multi-chunk: 5000 rows, distinct 1, max 999.
- Mid-chunk boundary: 1001/2000/499 distribution; transition at index 1001.
- Large-then-small: 3001 rows of value 1, then 10 single-row runs; distinct count 10 on tail.
- Type variants: min/max for int8, exact counts for int64 extremes, distinct counts for floats, string distribution.
- Exact chunk sizes (2048/4096/2049/2047) counts and distinct values match generator.
- Multi-runs-in-one-chunk and alternating-cross-chunk counts and boundary checks match generator.
- Very small dataset: explicit 3-row expansion.
- Aggregation checks (sum index, group by values) match run lengths.

---

## test/sql/multithreading.test
Data source: `test/data/create_multithreading_test.py`.

Generator summary:
- 10 detectors, 1000 rows each.
- `time_offset = row*1000 + detector_id*100`.
- `event_id = row + detector_id*10000`.
- RSE: 10 runs, each 100 rows; event_time_zero = 1_000_000_000 + detector_id*1_000_000 + i*50_000.

All expected outputs follow from those formulas:
- UNION ALL counts: 2 detectors → 2000, 5 detectors → 5000, 10 detectors → 10000.
- MIN time_offset = 100 (detector_1 row 0), MAX event_id = 100999 (detector_10 row 999).
- Aggregations and UNION ALL across detectors match arithmetic ranges.
- RSE expansions: 10 runs per detector → 10 distinct time_zero per detector; counts scale to 30, 100, etc. per query.
- Filters on event_id and time_zero match formulas.
- GROUP BY and window tests are consistent with event_id ranges.
- h5_tree concurrency count: each detector has 4 datasets → 12 total for 3 detectors.

---

## test/sql/large/h5db.test
Data source: `test/data/large/create_large_simple.py`.

Key generator facts:
- NUM_ROWS = 10,000,000 (integers 0..9,999,999).
- Sums: sum 0..N-1 = N*(N-1)/2 = 49,999,995,000,000.
- int8 cycles -128..127; uint8 cycles 0..255.
- array_3d limited to 1,000,000 rows; array_4d limited to 500,000 rows.
- array_2d/matrix rows follow `np.arange` reshapes.

Expected outputs in tests match these formulas:
- Counts for all datasets match NUM_ROWS or the reduced 3D/4D sizes.
- Sum/min/max expectations match arithmetic for ranges.
- Type detection matches HDF5 types (int8→TINYINT, uint8→UTINYINT, float32→FLOAT, arrays dimensions reversed).
- Array element checks match row-major order (first row values).
- Grouped bucket counts (integers // 1,000,000) yield 10 buckets of 1,000,000 each.
- Join of integers and group1/data2 where equal returns all rows because both are 0..N-1.

---

## test/sql/large/predicate_pushdown.test
Data source: `test/data/large/create_large_pushdown.py`.

Generator summary:
- 10,000,000 rows, 5 runs of 2,000,000 each.
- int_rse_values [10,20,30,40,50], floats [1.5..5.5], int64 [100..500], regular 0..9,999,999.

Expected outputs are scaled versions of small pushdown:
- `=30` → 2,000,000 rows, min 4,000,000, max 5,999,999.
- `>10` / `>=20` → 8,000,000 rows, min 2,000,000.
- `<20` → 2,000,000 rows, max 1,999,999.
- `<=20` → 4,000,000 rows, max 3,999,999.
- BETWEEN ranges return 2M/4M/6M/10M rows as indicated.
- Unsorted RSE and NOT/OR tests reflect post-filtering only.

All numeric expected results in the file match the run boundaries and comments.

---

## test/sql/large/rse_edge_cases.test
Data source: `test/data/large/create_large_rse_edge_cases.py`.

All outputs match generator:
- Single row dataset yields (0,42) count 1.
- Single-entry runs: 1,000,000 rows; values cycle 1000..1999; sum 1,499,500,000.
- Chunk-aligned: 10.24M rows; 1-10 cycling, 1,024,000 rows per value; boundary checks match.
- Large single run: 10M rows all 777; sum 7,770,000,000.
- Alternating runs: 10M rows, 2048-length runs; counts 5,000,832 vs 4,999,168 (due to run count parity).
- Mid-chunk: 10M rows, 1500-length runs; 10 distinct values 10-19; count for values>=15 ≈ 4,997,500.
- Many small runs: 10M rows; values 500..999; avg ~751.
- Type variants: float64 min/max 1/100, string counts 10,000, int64 min/max 0/499,000,000.
- Combined joins and sums follow generator values and run lengths.

---

## test/sql/large/multithreading.test
Data source: `test/data/large/create_large_multithreading.py`.

Generator summary:
- 10 detectors × 2,000,000 rows.
- event_id for detector k ranges [(k-1)*2,000,000 .. k*2,000,000-1].
- time_offset = row*1000 + detector_id*100.
- RSE runs: 200 per detector (10,000 rows/run).

Expected outputs:
- UNION ALL counts match total rows (e.g., 4M for 2 detectors, 10M for 5).
- Min/max event_id and time_offset match formulas.
- RSE distinct counts (240/280/380) match overlap of time_zero sets across detectors.
- Grouped bucket counts for event_id // 100,000 are 100,000 per bucket for 3 detectors.
- Filtered counts match ranges (e.g., event_id < 1,000,000 yields 1,000,000 rows).

---

## test/sql/large/nd_cache.test
Data source: `test/data/create_nd_cache_test.py`.

Generator fills each dataset row `i` with values broadcast from `i`. Therefore:
- All datasets have 1,000,000 rows.
- Spot checks read element `[1]` (or nested `[1][1]`) and expect the row index at the given OFFSET.
These match the generator’s `_fill_dataset` behavior.

---

## test/sql/rse_validation_errors.test
Data source: `test/data/create_rse_invalid_test.py`.

Expected errors match validation in `src/h5_read.cpp`:
- non-increasing run_starts → error.
- run_start beyond num_rows → error.
- size mismatch → error.
- non-integer run_starts → error.

---

## test/sql/unsupported_types.test
Data source: `test/data/create_unsupported_types_test.py`.

- h5_tree dtype reporting: float16 → `float16`, enum → `enum`, compound → `compound` via `H5TypeToString`.
- h5_read errors: float16 (size 2), enum (type class 8), compound (type class 6) match `H5TypeToDuckDBType` exceptions.

---

## test/sql/attrs_edge_cases.test
Data source: `test/data/create_attrs_edge_cases.py`.

- Empty dataset/group attributes → `Object has no attributes` (explicit check).
- vlen + fixed string attributes return `"variable"` and `"fixed"`.
- 2D attribute → error `unsupported multidimensional dataspace`.
- enum/compound/float16 attributes → errors from `H5AttributeTypeToDuckDBType` / `H5TypeToDuckDBType`.

---

## test/sql/empty_scalar.test
Data source: `test/data/create_empty_scalar_test.py`.

- h5_tree shapes: scalar `()` and empty 1D `(0)` match HDF5 shapes.
- h5_read on scalar → error `Dataset has no dimensions`.
- empty datasets yield count 0.
- min row count across datasets: short=2, long=5 → count 2; empty + long → count 0.
- h5_index + RSE only → error (no regular dataset).

---

## test/sql/names_edge_cases.test
Data source: `test/data/create_names_edge_cases.py`.

- h5_tree finds datasets with spaces/unicode paths; dtype int32 matches generator.
- h5_read handles paths with spaces/unicode; sums: with space (0+1+2=3), group data (0+1=1), unicode_µ (0+1+2+3=6).
- Alias collision → binder error for duplicate column names, as expected.

---

## test/sql/multidim_mismatch.test
Data source: `test/data/create_multidim_mismatch_test.py`.

- h5_tree shapes `(4,3)` and `(6,2,2)` match generator.
- h5_read row count uses min rows: 4.
- First row values: array_2d[1]=0, array_3d[1][1]=0 (row-major).

---

## test/sql/empty_scalar.test, test/sql/names_edge_cases.test, test/sql/multidim_mismatch.test
These are newly added tests; results match generator logic as described above.
