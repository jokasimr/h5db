# Test Data Files

This directory contains test data files and their generators for the h5db test suite.

## Generating Test Data

To regenerate all test data files:

```bash
./test/data/generate_all_test_data.sh
```

This will create all `.h5` files needed for the test suite (~2.2 GB total).

To ensure test data exists (generate only if missing):

```bash
./test/data/ensure_test_data.sh
```

## Test Data Files and Generators

### Core Test Files (`test/data/`)

| File | Generator | Size | Purpose |
|------|-----------|------|---------|
| `simple.h5` | `create_simple_types_multidim.py` | 12 KB | Basic datasets, groups, nested data |
| `types.h5` | `create_simple_types_multidim.py` | 11 KB | Type system tests (int8/16/32/64, uint*, float32/64, strings) |
| `multidim.h5` | `create_simple_types_multidim.py` | 4 KB | Multi-dimensional arrays (2D, 3D, 4D) |
| `run_encoded.h5` | `create_run_encoded_test.py` | 33 KB | RSE (Run-Sequence Encoding) functionality |
| `with_attrs.h5` | `create_attrs_test.py` | 8 KB | HDF5 attributes on datasets and groups |
| `multithreading_test.h5` | `create_multithreading_test.py` | 112 KB | Parallel execution with 10 detectors |
| `pushdown_test.h5` | `create_pushdown_test.py` | 24 KB | Predicate pushdown optimization |
| `rse_edge_cases.h5` | `create_rse_edge_cases.py` | 168 KB | RSE edge cases (chunk boundaries, constants, etc.) |
| `rse_invalid.h5` | `create_rse_invalid_test.py` | 4 KB | RSE validation errors (invalid run_starts, sizes) |
| `unsupported_types.h5` | `create_unsupported_types_test.py` | 4 KB | Unsupported HDF5 types (enum, compound, float16) |
| `attrs_edge_cases.h5` | `create_attrs_edge_cases.py` | 4 KB | Attribute edge cases (empty, vlen, multidim) |
| `empty_scalar.h5` | `create_empty_scalar_test.py` | 2 KB | Empty and scalar datasets (row-count edge cases) |
| `names_edge_cases.h5` | `create_names_edge_cases.py` | 2 KB | Dataset names with spaces/unicode |
| `multidim_mismatch.h5` | `create_multidim_mismatch_test.py` | 3 KB | Multi-dimensional row-count mismatch |
| `nd_cache_test.h5` | `create_nd_cache_test.py` | 310 MB | N-D cache coverage with varied chunking |
| `large_rse_test.h5` | `create_large_rse_test.py` | 16 MB | Large RSE multithreading regression tests |

### Large Test Files (`test/data/large/`)

| File | Generator | Size | Purpose |
|------|-----------|------|---------|
| `large_simple.h5` | `large/create_large_simple.py` | 1.3 GB | 10M row version of simple.h5 |
| `large_multithreading.h5` | `large/create_large_multithreading.py` | 153 MB | 2M rows × 10 detectors with RSE |
| `large_pushdown_test.h5` | `large/create_large_pushdown.py` | 115 MB | 10M row version of pushdown_test.h5 |
| `large_rse_edge_cases.h5` | `large/create_large_rse_edge_cases.py` | 266 MB | 10M row version of rse_edge_cases.h5 |

### Benchmark Files (Not Part of Test Suite)

Benchmark data generation lives under `benchmark/` and is not part of the test suite.

## File Structure

```
test/data/
├── README.md                           # This file
├── generate_all_test_data.sh          # Script to regenerate all test data
├── ensure_test_data.sh                # Generate all data if any file is missing
│
├── create_simple_types_multidim.py    # Creates: simple.h5, types.h5, multidim.h5
├── create_run_encoded_test.py         # Creates: run_encoded.h5
├── create_attrs_test.py               # Creates: with_attrs.h5
├── create_multithreading_test.py      # Creates: multithreading_test.h5
├── create_pushdown_test.py            # Creates: pushdown_test.h5
├── create_rse_edge_cases.py           # Creates: rse_edge_cases.h5
├── create_rse_invalid_test.py         # Creates: rse_invalid.h5
├── create_unsupported_types_test.py   # Creates: unsupported_types.h5
├── create_attrs_edge_cases.py         # Creates: attrs_edge_cases.h5
├── create_empty_scalar_test.py        # Creates: empty_scalar.h5
├── create_names_edge_cases.py         # Creates: names_edge_cases.h5
├── create_multidim_mismatch_test.py   # Creates: multidim_mismatch.h5
├── create_nd_cache_test.py            # Creates: nd_cache_test.h5
├── create_large_rse_test.py           # Creates: large_rse_test.h5
│
├── simple.h5                          # Generated files (in .gitignore)
├── types.h5
├── multidim.h5
├── nd_cache_test.h5
├── large_rse_test.h5
├── ... (other .h5 files)
│
└── large/                             # Generator scripts for large datasets
    ├── create_large_simple.py
    ├── create_large_multithreading.py
    ├── create_large_pushdown.py
    ├── create_large_rse_edge_cases.py

test/data/large/
├── large_simple.h5                    # Generated files (in .gitignore)
├── large_multithreading.h5
├── large_pushdown_test.h5
└── large_rse_edge_cases.h5
```

## Development

When adding new test data files:

1. Create a generator script in `test/data/` or `test/data/large/`
2. Add the generator to `generate_all_test_data.sh`
3. Update this README with the new file
4. Ensure the `.h5` file is in `.gitignore` (not committed to git)

## Notes

- All `.h5` and `.hdf` files should be in `.gitignore`
- Only generator scripts (`.py`) should be committed to git
- Test data files are regenerated as needed by developers or CI/CD
- The virtual environment must be activated before running generators
