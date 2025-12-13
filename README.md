# H5DB - HDF5 Extension for DuckDB

A DuckDB extension for reading HDF5 (Hierarchical Data Format 5) files directly using SQL queries.

This repository is based on https://github.com/duckdb/extension-template.

---

## Overview

H5DB enables DuckDB to read data from HDF5 files, a widely-used format in scientific computing, machine learning, and data science. Query HDF5 datasets using standard SQL without conversion to other formats.

### Features

- **Read HDF5 datasets**: Access datasets from HDF5 files using table-valued functions
- **Hierarchical navigation**: Read datasets from nested groups with full path support
- **Multiple datasets**: Read and combine multiple datasets in a single query
- **Run-start encoding**: Efficient reading of run-length encoded data with automatic expansion
- **Type mapping**: Automatic conversion between HDF5 and DuckDB data types
- **Multi-dimensional arrays**: Support for N-dimensional datasets using DuckDB's array types
- **Metadata access**: Inspect file structure and dataset attributes

### Quick Start

```sql
-- List all datasets and groups in an HDF5 file
SELECT * FROM h5_tree('data.h5');

-- Read a dataset
SELECT * FROM h5_read('data.h5', '/dataset_name');

-- Read from nested groups
SELECT * FROM h5_read('data.h5', '/group1/subgroup/dataset');

-- Read multiple datasets (horizontal stacking)
SELECT * FROM h5_read('data.h5', '/dataset1', '/dataset2');

-- Read run-start encoded data (automatic expansion)
SELECT * FROM h5_read(
    'data.h5',
    '/timestamp',                                      -- Regular column
    h5_rse('/state_run_starts', '/state_values')      -- Run-encoded column
);

-- Combine regular and run-encoded columns with aggregation
SELECT status, COUNT(*) as count, AVG(measurement) as avg_val
FROM h5_read(
    'experiment.h5',
    '/data/measurement',
    h5_rse('/data/status_starts', '/data/status_vals')
)
GROUP BY status;
```

### Documentation

- **[RSE_USAGE.md](RSE_USAGE.md)** - Complete guide to run-start encoding support
- **[PLAN.md](PLAN.md)** - Implementation roadmap and current status
- **[TEST_SUITE_SUMMARY.md](TEST_SUITE_SUMMARY.md)** - Test coverage details


## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/h5db/h5db.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `h5db.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Once loaded, you can use the H5DB functions to query HDF5 files:

```sql
-- List all datasets in a file
D SELECT * FROM h5_tree('example.h5');

-- Read a regular dataset
D SELECT * FROM h5_read('example.h5', '/measurements');

-- Read multiple datasets
D SELECT * FROM h5_read('example.h5', '/timestamps', '/measurements');

-- Read run-start encoded data (expands automatically)
D SELECT * FROM h5_read(
    'example.h5',
    '/timestamps',
    h5_rse('/state_run_starts', '/state_values')
);
```

For detailed documentation on run-start encoding, see **[RSE_USAGE.md](RSE_USAGE.md)**.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL h5db;
LOAD h5db;
```

## Setting up CLion

### Opening project
Configuring CLion with this extension requires a little work. Firstly, make sure that the DuckDB submodule is available.
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

### Debugging
To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`, and CMake Options to which the following flag should be added, with the path to the extension CMakeList:

```
-DDUCKDB_EXTENSION_CONFIGS=<path_to_the_exentension_CMakeLists.txt>
```

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`. Note that it is recommended to use the `unittest` executable for testing/development within CLion. The actual DuckDB CLI currently does not reliably work as a run target in CLion.
