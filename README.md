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
- Table-valued `h5_tree(...)`, `h5_ls(...)`, and `h5_read(...)` accept single files, local/SFTP glob patterns, or
  `LIST(VARCHAR)` filename inputs.
- Supports reading HDF5 attributes on objects and the file root.
- Supports path-complete namespace listing with `h5_tree(...)`.
- Supports shallow group listing with table and scalar `h5_ls(...)`.
- Supports projecting selected HDF5 attributes as extra columns in `h5_tree(...)` and `h5_ls(...)`.

## Core Functions

- `h5_read(filename_or_filenames, datasets_or_definitions...)`
  Reads one or more datasets as DuckDB columns. Supports regular datasets, special column encodings such as
  "run start encoded" columns (see `h5_rse()`), and virtual index columns (see `h5_index()`).
- `h5_tree(filename_or_filenames, projected_attributes...)`
  Recursively lists namespace entries with `path`, `type`, `dtype`, and `shape`. Output is path-oriented: if multiple
  paths resolve to the same object, each path appears as its own row.
- `h5_ls(filename_or_filenames[, group_path], projected_attributes...)`
  Lists only the immediate children of a group. The table form returns the same row shape as `h5_tree`; the scalar
  form returns a `MAP(VARCHAR, STRUCT(...))` keyed by child name.
- `h5_attributes(filename, object_path)`
  Reads attributes from an object or the file root as a single wide row.

For a practical guide to the main workflows, see [docs/USAGE_GUIDE.md](docs/USAGE_GUIDE.md).
For the full API, see [docs/API.md](docs/API.md).

## Quick Start

In DuckDB, install and load the extension:

```sql
INSTALL h5db FROM community;
LOAD h5db;
```

Then run queries such as:

```sql
-- Read one dataset
FROM h5_read('data.h5', '/measurements');

-- Read multiple datasets side by side
FROM h5_read('data.h5', '/timestamps', '/temperatures');

-- Add a virtual row index
FROM h5_read('data.h5', h5_index(), '/measurements');

-- Read a remote file
FROM h5_read('https://example.com/data.h5', '/dataset_name');

-- Read matching local files
FROM h5_read('runs/run_*.h5', '/counts');

-- Expand an explicit list of exact files and/or patterns in order
FROM h5_tree([
    'runs/calibration.h5',
    'runs/run_*.h5'
]);

-- Inspect the file structure
FROM h5_tree('data.h5');

-- Inspect the file structure and project selected attributes
SELECT path, type, NX_class
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class')
);

-- Project all attributes as one map-valued column
SELECT path, h5_attr
FROM h5_tree(
    'data.h5',
    h5_attr()
);

-- List only the root group's immediate children
FROM h5_ls('data.h5');

-- List the immediate children of a specific group
FROM h5_ls('data.h5', '/entry/instrument');

-- Return a map of immediate children keyed by child name
SELECT h5_ls('data.h5', '/entry/instrument');

-- Read attributes
FROM h5_attributes('data.h5', '/measurements');

-- Read run-start encoded data
FROM h5_read(
    'experiment.h5',
    '/timestamp',
    h5_rse('/state_run_starts', '/state_values')
);

-- Rename a column definition
FROM h5_read(
    'data.h5',
    h5_alias('idx', h5_index()),
    '/measurements'
);

-- Rename a projected h5_tree attribute column
SELECT path, time
FROM h5_tree(
    'data.h5',
    h5_alias('time', h5_attr('count_time'))
);
```

## Remote Access

All h5db functions accept single local paths or remote URLs as `filename`.

The table-valued `h5_tree(...)`, `h5_ls(...)`, and `h5_read(...)` also accept:

- a local or `sftp://` glob pattern
- a `LIST(VARCHAR)` of exact filenames/URLs and/or glob patterns

Multi-file semantics:

- a single glob expands to lexicographically sorted matches
- list entries are expanded left-to-right
- duplicate files are preserved and are processed more than once
- `h5_read(...)` concatenates rows file by file
- `h5_index()` is the outermost-dimension row index within each matched file, so it starts at `0` for each file
- table `h5_tree(...)`, table `h5_ls(...)`, and `h5_read(...)` expose a hidden virtual `filename` column; refer to
  it explicitly when you need file provenance
- `filename := true` adds `filename` to the visible output schema, so it appears in `FROM ...` / `SELECT *`
- `filename := 'source_file'` adds the same visible filename column but uses the provided column name instead of `filename`
  and replaces the hidden `filename` binding with that visible name
- `h5_read(...)` requires compatible column definitions across all matched files
- `h5_attributes(...)` and the scalar `h5_ls(...)` still operate on one file at a time

Globs support the usual `*`, `?`, and bracket classes, plus one recursive `**`
segment per pattern. A pattern that matches no files raises an error.

When several files contain the same HDF5 path, use `filename` to distinguish them:

```sql
SELECT filename, path
FROM h5_tree('runs/**/*.h5')
WHERE path = '/entry/data';

FROM h5_read('runs/run_*.h5', '/entry/data', filename := true);
```

- DuckDB-backed remote schemes such as `http://`, `https://`, `s3://`, `s3a://`, `s3n://`, `r2://`, `gcs://`,
  `gs://`, and `hf://` use DuckDB's filesystem stack and benefit from DuckDB's remote caching features.
- `sftp://` URLs are supported through h5db's built-in SFTP backend.
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

FROM h5_read(
    'sftp://beamline.example.org/data/run001.h5',
    '/entry/data'
);

FROM h5_read(
    'sftp://beamline.example.org/data/run_*.h5',
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

For contributor workflows that run `make test` or the SFTP interaction harness, set up and
activate the repo venv first:

```bash
./scripts/setup-dev-env.sh
source venv/bin/activate
make -j8
```

### Build Outputs

- `./build/release/duckdb`
  DuckDB shell with `h5db` loaded.
- `./build/release/test/unittest`
  SQLLogicTest runner.
- `./build/release/extension/h5db/h5db.duckdb_extension`
  Loadable extension artifact.

## Current Limitations

- Compound, enum, reference, opaque, bitfield, time-like, and non-string variable-length HDF5 types are not supported.
- Datasets with more than 4 dimensions are not supported.
- Multi-dimensional string datasets are not supported.
- Attribute multidimensional dataspaces are not supported.

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
- [docs/USAGE_GUIDE.md](docs/USAGE_GUIDE.md): practical usage guide for the main workflows
- [docs/API.md](docs/API.md): function reference, settings, type mapping, and limitations
- [docs/RSE_USAGE.md](docs/RSE_USAGE.md): detailed guide to run-start encoding support
- [docs/developer/DEVELOPER.md](docs/developer/DEVELOPER.md): building, testing, debugging, and project layout
