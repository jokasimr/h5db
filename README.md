# H5DB - HDF5 Extension for DuckDB

A DuckDB extension for reading HDF5 (Hierarchical Data Format 5) files directly using SQL queries.

This repository is based on https://github.com/duckdb/extension-template.

---

## Overview

H5DB enables DuckDB to read data from HDF5 files, a widely-used format in scientific computing, machine learning, and data science. Query HDF5 datasets using standard SQL without conversion to other formats.

### Features

- **Browse file structure**: List groups and datasets with `h5_tree()`
- **Read datasets**: Access data from HDF5 files with `h5_read()`
- **Read attributes**: Access dataset and group attributes with `h5_attributes()`
- **Hierarchical navigation**: Full support for nested groups
- **Multiple datasets**: Read and combine multiple datasets in a single query
- **Run-start encoding**: Efficient reading of run-length encoded data with automatic expansion
- **Projection pushdown**: Only read columns actually needed by your query for better performance
- **Predicate pushdown**: Range filters with static constants on RSE and `h5_index()` columns reduce I/O (e.g., `col > 10`, `col BETWEEN 5 AND 20`)
- **Type mapping**: Automatic conversion between HDF5 and DuckDB data types
- **Multi-dimensional arrays**: Support for 1D-4D datasets using DuckDB's array types
- **Virtual index column**: Add a row index column with `h5_index()`
- **Column aliasing**: Rename columns with `h5_alias()`

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

-- Add a virtual index column
SELECT * FROM h5_read('data.h5', h5_index(), '/dataset1');

-- Rename a column definition
SELECT * FROM h5_read('data.h5', h5_alias('idx', h5_index()), '/dataset1');

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

-- Read attributes from a dataset or group
SELECT * FROM h5_attributes('data.h5', '/dataset_name');
```

### Documentation

- **[API.md](API.md)** - Complete API reference for all functions
- **[RSE_USAGE.md](RSE_USAGE.md)** - Complete guide to run-start encoding support
- **[docs/DEVELOPER.md](docs/DEVELOPER.md)** - Developer guide (building, testing, development)
- **[CLAUDE.md](CLAUDE.md)** - Instructions for AI agents working on this project


## Building

### Quick Start

```bash
# 1. Set up VCPKG (one-time setup)
# (Not in this directory!)
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/scripts/buildsystems/vcpkg.cmake
cd ..
# If VCPKG is already installed VCPKG_TOOLCHAIN_PATH should exist.
# If it does not exist it might be stored in an `.env` file.
# If it isn't then the path might be `../vcpkg/scripts/buildsystems/vcpkg.cmake`. But you need to verify if that is the case.

# 2. Clone and build h5db
git clone <repository-url>
cd h5db
git submodule update --init --recursive

# 3. Configure environment (update .env with your vcpkg path)
source venv/bin/activate

# 4. Build
make -j8
```

The main binaries will be built in:
- `./build/release/duckdb` - DuckDB shell with h5db extension loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/h5db/h5db.duckdb_extension` - Loadable extension

**For detailed build instructions, testing, and development workflows, see [docs/DEVELOPER.md](docs/DEVELOPER.md).**

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

-- Read attributes
D SELECT * FROM h5_attributes('example.h5', '/dataset_name');
```

For detailed documentation on run-start encoding, see **[RSE_USAGE.md](RSE_USAGE.md)**.

## Running the Tests

```bash
# Run all tests (generates test data if missing)
./build/release/test/unittest "test/sql/*"

# Or use the makefile target (also generates test data if missing)
make test
```

**For detailed testing instructions, test creation, and Python script usage, see [docs/DEVELOPER.md](docs/DEVELOPER.md).**

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
