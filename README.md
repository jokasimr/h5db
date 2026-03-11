# h5db: HDF5 Extension for DuckDB

`h5db` lets DuckDB query HDF5 files directly with SQL. It is aimed at analytics-style access to HDF5 data: inspect
file structure, read datasets as columns, read attributes, and work with remote files through DuckDB `httpfs`.

## Highlights

- Reads local or remote (`https://`, `s3://`, ...) HDF5 files directly from SQL.
- Maps numeric datasets, string datasets, and 1D-4D array datasets into DuckDB types.
- Multiple dataset can be stacked horizontally to make a table.
- Scalar datasets are treated as constant columns.
- Supports projection pushdown.
- Supports row-range predicate pushdown for `h5_index()` and run-start encoded columns.
- Supports reading HDF5 attributes on groups and datasets.

## Core Functions

- `h5_read(filename, datasets_or_definitions...)`
  Reads one or more datasets as DuckDB columns. Supports regular datasets, special column encodings such as
  "run start encoded" columns (see `h5_rse()`), and virtual index columns (see `h5_index()`).
- `h5_tree(filename)`
  Lists groups and datasets with `path`, `type`, `dtype`, and `shape`.
- `h5_attributes(filename, object_path)`
  Reads attributes from a dataset or group as a single wide row.

For the full API, see [API.md](API.md).

## Quick Start

In DuckDB, install and load the extension:

```sql
INSTALL h5db FROM community;
LOAD h5db;
```

Then run queries such as:

```sql
-- Inspect the file structure
SELECT * FROM h5_tree('data.h5');

-- Read one dataset
SELECT * FROM h5_read('data.h5', '/measurements');

-- Read multiple datasets side by side
SELECT * FROM h5_read('data.h5', '/timestamps', '/temperatures');

-- Add a virtual row index
SELECT * FROM h5_read('data.h5', h5_index(), '/measurements');

-- Read run-start encoded data
SELECT * FROM h5_read(
    'experiment.h5',
    '/timestamp',
    h5_rse('/state_run_starts', '/state_values')
);

-- Rename a column definition
SELECT * FROM h5_read(
    'data.h5',
    h5_alias('idx', h5_index()),
    '/measurements'
);

-- Read attributes
SELECT * FROM h5_attributes('data.h5', '/measurements');

-- Read a remote file
SELECT * FROM h5_read('https://example.com/data.h5', '/dataset_name');
```

## Build From Source

### Prerequisites

- `vcpkg`
- `VCPKG_TOOLCHAIN_PATH` pointing to `vcpkg/scripts/buildsystems/vcpkg.cmake`
- Git submodules initialized

### Quick Build

```bash
# 1. Install vcpkg (one-time setup, outside this repo)
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH="$(pwd)/scripts/buildsystems/vcpkg.cmake"
cd ..

# 2. Clone and build h5db
git clone https://github.com/jokasimr/h5db.git
cd h5db
git submodule update --init --recursive
make -j8
```

If you prefer not to export `VCPKG_TOOLCHAIN_PATH` in your shell, put it in a repo-root `.env` file instead. See
[docs/DEVELOPER.md](docs/DEVELOPER.md) for the full setup and troubleshooting guide.

### Build Outputs

- `./build/release/duckdb`
  DuckDB shell with `h5db` loaded.
- `./build/release/test/unittest`
  SQLLogicTest runner.
- `./build/release/extension/h5db/h5db.duckdb_extension`
  Loadable extension artifact.

## Behavior Notes

- `swmr := true` enables HDF5 SWMR read mode for local files.
- Remote URLs accept `swmr`, but remote opens use the DuckDB-backed remote VFD as immutable snapshots served by
  `httpfs`; `H5F_ACC_SWMR_READ` is not used on that path.
- If multiple non-scalar regular datasets are read together, `h5_read()` uses the minimum outer dimension as the output
  row count.
- If the target object has no attributes, `h5_attributes()` raises `IO Error: Object has no attributes: ...`.

## Current Limitations

- Compound, enum, reference, opaque, bitfield, time-like, and non-string variable-length HDF5 types are not supported.
- Datasets with more than 4 dimensions are not supported.
- Multi-dimensional string datasets are not supported.
- Attribute string arrays are not supported.
- Attribute multidimensional dataspaces are not supported.

See [API.md](API.md) for full type-mapping details and error behavior.

## Testing

```bash
# Full suite: local tests + rewritten remote suite
make test

# Local SQLLogicTests only
./build/release/test/unittest "test/sql/*" "~test/sql/remote/*"

# Rewritten remote URL suite via the local range-capable HTTP server
make test_remote_http
```

Notes:

- `make test` ensures missing HDF5 fixtures exist before running tests.
- On macOS, the Makefile currently skips the remote HTTP portion of `make test`; use Linux to exercise the rewritten
  remote suite.

For targeted test runs, test-data generation, and debugging workflows, see [docs/DEVELOPER.md](docs/DEVELOPER.md) and
[test/README.md](test/README.md).

## Documentation

- [API.md](API.md): function reference, settings, type mapping, and limitations
- [RSE_USAGE.md](RSE_USAGE.md): detailed guide to run-start encoding support
- [docs/DEVELOPER.md](docs/DEVELOPER.md): building, testing, debugging, and project layout
