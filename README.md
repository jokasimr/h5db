# h5db: HDF5 Extension for DuckDB

`h5db` lets DuckDB query HDF5 files directly with SQL. It is aimed at analytics-style access to HDF5 data: inspect
file structure, read datasets as columns, read attributes, and work with remote files through DuckDB's filesystem
stack or SFTP.

## Highlights

- Reads local or remote (`https://`, `s3://`, `sftp://`, ...) HDF5 files directly from SQL.
- Maps numeric datasets, string datasets, and 1D-4D array datasets into DuckDB types.
- Multiple datasets can be stacked horizontally to make a table.
- Scalar datasets are treated as constant columns.
- Supports projection pushdown in `h5_read(...)`.
- Supports row-range predicate pushdown for `h5_index()` and run-start encoded columns.
- Supports reading HDF5 attributes on objects and the file root.
- Supports path-complete namespace listing with `h5_tree(...)`.
- Supports shallow group listing with table and scalar `h5_ls(...)`.
- Supports projecting selected HDF5 attributes as extra columns in `h5_tree(...)` and `h5_ls(...)`.

## Core Functions

- `h5_read(filename, datasets_or_definitions...)`
  Reads one or more datasets as DuckDB columns. Supports regular datasets, special column encodings such as
  "run start encoded" columns (see `h5_rse()`), and virtual index columns (see `h5_index()`).
- `h5_tree(filename, projected_attributes...)`
  Recursively lists namespace entries with `path`, `type`, `dtype`, and `shape`. Output is path-oriented: if multiple
  paths resolve to the same object, each path appears as its own row.
- `h5_ls(filename[, group_path], projected_attributes...)`
  Lists only the immediate children of a group. The table form returns the same row shape as `h5_tree`; the scalar
  form returns a `MAP(VARCHAR, STRUCT(...))` keyed by child name.
- `h5_attributes(filename, object_path)`
  Reads attributes from an object or the file root as a single wide row.

For the full API, see [docs/API.md](docs/API.md).

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

-- Inspect the file structure and project selected attributes
SELECT path, type, NX_class
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class', NULL::VARCHAR)
);

-- List only the root group's immediate children
SELECT * FROM h5_ls('data.h5');

-- List the immediate children of a specific group
SELECT * FROM h5_ls('data.h5', '/entry/instrument');

-- Return a map of immediate children keyed by child name
SELECT h5_ls('data.h5', '/entry/instrument');

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

-- Rename a projected h5_tree attribute column
SELECT path, time
FROM h5_tree(
    'data.h5',
    h5_alias('time', h5_attr('count_time', 0::DOUBLE))
);

-- Read a remote file
SELECT * FROM h5_read('https://example.com/data.h5', '/dataset_name');
```

## Remote Access

All h5db functions accept local paths or remote URLs as `filename`.

- DuckDB-backed remote schemes such as `http://`, `https://`, `s3://`, `s3a://`, `s3n://`, `r2://`, `gcs://`,
  `gs://`, and `hf://` use DuckDB's filesystem stack and benefit from DuckDB's remote caching features.
- `sftp://` URLs are supported through h5db's built-in SFTP backend on POSIX platforms. On Windows, `sftp://` is not
  supported.
- Remote opens are treated as immutable snapshots. `swmr := true` is accepted for API consistency, but remote paths do
  not use `H5F_ACC_SWMR_READ`.

To read over SFTP, create a DuckDB secret of type `sftp` whose scope matches the URL you want to access:

```sql
CREATE OR REPLACE SECRET beamline_sftp (
    TYPE sftp,
    SCOPE 'sftp://beamline.example.org/',
    USERNAME 'alice',
    PASSWORD 'secret',
    KNOWN_HOSTS_PATH '/home/alice/.ssh/known_hosts'
);

SELECT * FROM h5_read(
    'sftp://beamline.example.org/data/run001.h5',
    '/entry/data'
);
```

SFTP secrets require:

- `USERNAME`
- exactly one of `PASSWORD` or `KEY_PATH`
- at least one of `KNOWN_HOSTS_PATH` or `HOST_KEY_FINGERPRINT`

Optional SFTP secret fields:

- `KEY_PASSPHRASE`
- `PORT` (default `22`)
- `HOST_KEY_ALGORITHMS`

For key-based auth, replace `PASSWORD` with `KEY_PATH` and optionally `KEY_PASSPHRASE`. If you use
`HOST_KEY_FINGERPRINT`, provide the lowercase hex SHA1 host-key fingerprint. See [docs/API.md](docs/API.md) for the
full SFTP secret reference.

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
[docs/developer/DEVELOPER.md](docs/developer/DEVELOPER.md) for the full setup and troubleshooting guide.

### Build Outputs

- `./build/release/duckdb`
  DuckDB shell with `h5db` loaded.
- `./build/release/test/unittest`
  SQLLogicTest runner.
- `./build/release/extension/h5db/h5db.duckdb_extension`
  Loadable extension artifact.

## Behavior Notes

- `swmr := true` enables HDF5 SWMR read mode for local files.
- Remote URLs accept `swmr`, but remote opens use the DuckDB-backed remote VFD as immutable snapshots; remote paths do
  not use `H5F_ACC_SWMR_READ`.
- If multiple non-scalar regular datasets are read together, `h5_read()` uses the minimum outer dimension as the output
  row count.
- If the target object has no attributes, `h5_attributes()` raises `IO Error: Object has no attributes: ...`.
- In `h5_tree(...)`, projected attributes use the declared default when an object does not have the attribute.
- `h5_attr(name)` is shorthand for `h5_attr(name, NULL::VARIANT)`.
- In `h5_tree(...)`, rows are path-oriented and recursive: alias paths, dangling links, and external links can all
  appear as separate rows.
- In `h5_ls(...)`, only the immediate children of the requested group are returned.
- Table `h5_ls(...)` defaults the path to `/` if omitted.
- Scalar `h5_ls(...)` requires an explicit path and returns `NULL` for `NULL` path inputs.
- Scalar `h5_ls(...)` does not accept named parameters such as `swmr := true`; use `h5_ls_swmr(...)` or
  `SET h5db_swmr_default = true`.
- In `h5_tree(...)`, projected output names must be unique; duplicate projected names or collisions with `path`,
  `type`, `dtype`, or `shape` fail at bind time.

## Current Limitations

- Compound, enum, reference, opaque, bitfield, time-like, and non-string variable-length HDF5 types are not supported.
- Datasets with more than 4 dimensions are not supported.
- Multi-dimensional string datasets are not supported.
- Attribute string arrays are not supported.
- Attribute multidimensional dataspaces are not supported.
- Projected `h5_tree(...)` and `h5_ls(...)` attributes follow the same attribute type/limitation rules as
  `h5_attributes()`.

See [docs/API.md](docs/API.md) for full type-mapping details and error behavior.

## Testing

```bash
# Full suite: local tests + rewritten remote HTTP/SFTP suites
make test

# Local SQLLogicTests only
./build/release/test/unittest "test/sql/*" "~test/sql/remote/*"

# Rewritten remote URL suite via the local range-capable HTTP server
make test_remote_http

# Rewritten remote URL suite via the local SFTP server + interaction harness
make test_remote_sftp
```

Notes:

- `make test` ensures missing HDF5 fixtures exist before running tests.
- `make test` runs both remote harnesses.

For targeted test runs, test-data generation, and debugging workflows, see [docs/developer/DEVELOPER.md](docs/developer/DEVELOPER.md) and
[test/README.md](test/README.md).

## Documentation

- [docs/README.md](docs/README.md): documentation index
- [docs/API.md](docs/API.md): function reference, settings, type mapping, and limitations
- [docs/RSE_USAGE.md](docs/RSE_USAGE.md): detailed guide to run-start encoding support
- [docs/developer/DEVELOPER.md](docs/developer/DEVELOPER.md): building, testing, debugging, and project layout
