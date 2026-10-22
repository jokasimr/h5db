# H5DB Developer Guide

This guide covers the essential workflows for developing the h5db DuckDB extension.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Initial Setup](#initial-setup)
- [Building the Project](#building-the-project)
- [Running Tests](#running-tests)
- [Working with Python Scripts](#working-with-python-scripts)
- [Development Workflow](#development-workflow)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before you begin, ensure you have the following installed:

- **C++ compiler and standard library**: C++20 support, including `std::atomic::wait`
- **CMake**: 3.15+
- **Git**: For cloning repositories
- **Python 3**: For test data generation
- **ninja-build**: Fast build system (recommended)
- **ccache**: Compiler cache for faster rebuilds (recommended)

### Installing Prerequisites (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    python3 \
    python3-venv \
    ninja-build \
    ccache
```

---

## Initial Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd h5db
```

### 2. Initialize Git Submodules

The project includes DuckDB as a git submodule:

```bash
git submodule update --init --recursive
```

### 3. Set Up VCPKG (Dependency Management)

VCPKG is used to manage HDF5 and other dependencies.

**If you don't have vcpkg installed**:

```bash
# Clone vcpkg (recommended: same parent directory as h5db)
cd ..
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh

# Set the toolchain path environment variable
export VCPKG_TOOLCHAIN_PATH=`pwd`/scripts/buildsystems/vcpkg.cmake

# Go back to h5db directory
cd ../h5db
```

**Configure a local `.env` file**:

Create or update `.env` with your vcpkg path. The setup script below configures venv activation to source this file:

```bash
# Edit .env to set your vcpkg path
vim .env

# Example content:
# export VCPKG_TOOLCHAIN_PATH=/home/yourusername/personal/vcpkg/scripts/buildsystems/vcpkg.cmake
# export GEN=ninja
```

Or create it from scratch:

```bash
cat > .env << EOF
# H5DB Development Environment Variables

# VCPKG Toolchain Path - Required for dependency management
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build optimizations - Use ninja and ccache for fast builds
export GEN=ninja

# Optional: Increase ccache size if needed (default is 5GB)
# export CCACHE_MAXSIZE=10G
EOF
```

Replace `/path/to/vcpkg` with your actual vcpkg installation path. If you followed the steps above and installed vcpkg in the parent directory:

```bash
# From h5db directory, automatically set the path
export VCPKG_TOOLCHAIN_PATH=`cd ../vcpkg && pwd`/scripts/buildsystems/vcpkg.cmake
echo "export VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH" >> .env
echo "export GEN=ninja" >> .env
```

### 4. Set Up Python Virtual Environment

Create the development environment for test data generation, SFTP tests, and local tooling:

```bash
./scripts/setup-dev-env.sh
source venv/bin/activate
```

The setup script also configures activation to source `.env`. A venv created manually does not include that hook.

---

## Building the Project

### Quick Start

The source targets the DuckDB v2.0.0 release pinned by the submodule. From the repository root:

```bash
# Activate virtual environment (includes all tools and environment variables)
source venv/bin/activate

# Build the project (set GEN=ninja in .env to select Ninja)
make -j4
```

### Build Outputs

After a successful build, you'll find:

```
build/release/
├── duckdb                          # DuckDB CLI with h5db extension loaded
├── test/unittest                   # Test runner
└── extension/h5db/
    └── h5db.duckdb_extension      # Loadable extension binary
```

`CMakeLists.txt` uses DuckDB 2.0's `build_extension_library()` helper: h5db sources are compiled once into a static
archive, which is also used to link the loadable extension. HDF5 and libssh2 dependencies are attached to that
archive and inherited by its consumers. `extension_config.cmake` keeps h5db preloaded in the shell and test runner.

### Build Variants

```bash
# Release build (default, optimized)
make -j4

# Debug build (with debug symbols, no optimization)
make -j4 debug

# Release with debug info
make -j4 reldebug
```

### First Build vs. Incremental Builds

The first build installs missing vcpkg dependencies and compiles DuckDB and its configured extensions. Incremental
builds recompile affected sources and relink their consumers. Shared header and build-configuration changes can
trigger substantial rebuilds. Build times depend on the machine, dependencies, and compiler-cache state.

### Clean Build

If you need to start fresh:

```bash
# Clean all build artifacts
make clean

# Then rebuild
source venv/bin/activate && make -j4
```

---

## Running Tests

### Test Structure

The project uses DuckDB's SQLLogicTest framework. Tests are located in:

```
test/
├── sql/                       # SQLLogicTest files
└── data/
    ├── *.h5                   # HDF5 test files
    └── *.py                   # Scripts to generate test data

test/sql/large/                # Large SQLLogicTest files (slow)
```

### Running All Tests

```bash
# Full suite: local tests + rewritten remote HTTP/SFTP suites
make test

# Run the local SQLLogicTests directly
./build/release/test/unittest "test/sql/*" "~test/sql/remote/*"

# Optional: skip slow tests
./build/release/test/unittest "test/sql/*" "~test/sql/large/*" "~test/sql/remote/*"
```

If you run tests directly, ensure test data exists first:

```bash
./test/data/ensure_test_data.sh
```

### Running Specific Tests

```bash
# Run a specific local test file directly
./build/release/test/unittest "test/sql/<testfile>.test"

# Run the rewritten remote HTTP suite (includes test/sql/remote/*.test)
make test_remote_http

# Run the rewritten remote SFTP suite + dedicated interaction harness
make test_remote_sftp
```

### Running Individual Test Cases

You can filter tests by pattern:

```bash
# Run tests matching a pattern
./build/release/test/unittest "test/sql/<testfile>.test" -testcase="*pattern*"
```

### Using the Makefile Test Target

The Makefile provides a convenient test target:

```bash
# Full suite: local tests + remote HTTP/SFTP suites
make test
```

`make test` also ensures all HDF5 test data is present by running
`test/data/ensure_test_data.sh` before executing the tests.
It then runs the local SQLLogicTests, the rewritten remote HTTP suite (including `test/sql/remote/*.test`), and the
rewritten remote SFTP suite plus its dedicated interaction harness.

### Running Remote HTTP Test Suite

To run the suite against rewritten remote URLs (using the local range-capable HTTP server):

```bash
make test_remote_http
```

This target also runs `test/sql/remote/*.test` (auth, retries, redirects, timeout, truncation, corruption,
cache behavior, and simulated server/drop failures).

### Running Remote SFTP Test Suite

To run the suite against rewritten SFTP URLs (using the local rooted SFTP server):

```bash
make test_remote_sftp
```

This target rewrites the main SQL suite against `sftp://` URLs, starts the local SFTP test server, and then runs the
dedicated interaction harness in `test/scripts/run_sftp_interaction_tests.py`. The runner will use the repo venv when
present and otherwise falls back to `python3`/`python`, installing `paramiko` if needed.

For targeted SFTP work, run the harness scripts directly:

```bash
# Run a subset of rewritten SQLLogicTests through SFTP and skip the interaction harness
bash test/scripts/run_sftp_tests.sh --test-glob 'glob/*.test' --skip-interaction-tests

# Run selected interaction tests
./venv/bin/python test/scripts/run_sftp_interaction_tests.py \
  --duckdb-bin ./build/release/duckdb \
  SFTPInteractionTests.test_sftp_glob_skips_unlistable_literal_directory_component
```

### HTTP Caching

The DuckDB filesystem backend opens remote files with `RequestSizing::BY_READER`, since the HDF5 VFD already
coalesces small reads into requests of 30,000 bytes. Under DuckDB's default `AUTO` request sizing, this avoids rounding
every read to the 2 MiB cache grid. DuckDB still aligns remote ranges to its minimum block size (4 KiB by default) and
fetches files that fit within one maximum-size block in full.

All reads use the caching handle, which can fetch missing blocks asynchronously. There is no per-query byte cutoff.
DuckDB manages cache memory and eviction. With the default `external_file_cache_spill=false`, evicted cache payloads
are discarded. Enabling spilling lets eligible remote blocks use temporary storage, subject to `max_temp_directory_size`.

### SFTP Caching

The SFTP backend uses DuckDB's external file cache directly. DuckDB does not classify `sftp://` as a remote scheme,
so h5db applies remote validation, maximum block size, and spill policy explicitly. Cached ranges remain visible in
`duckdb_external_file_cache()` and use DuckDB's buffer manager for memory accounting and eviction.

Cache misses run synchronously under the HDF5 mutex, preserving serialization of shared libssh2 sessions. The VFD
continues to coalesce small reads into requests of 30,000 bytes; the external cache stores the requested byte ranges
without rounding them to another grid. Changing the maximum cache block size does not discard existing ranges. SFTP
does not apply the remote minimum block size or request-sizing grid. Validated reads require usable modification times;
`NO_VALIDATION` allows a warm cache to avoid opening another SFTP connection, accepting stale data if the file changes.

Connections are reused within a query and released after the query and its retained HDF5 handles finish. Default
validation still opens and stats the file for a later query, even if the contents are cached. Matching mtimes must be
more than ten seconds old to reuse data across opens; newly read ranges can be reused within the same open handle.

The block size matches libssh2's maximum SFTP READ payload. SFTP sockets enable `TCP_NODELAY` to reduce latency for
small requests.

For the complete read policy and measurement limitations, see [Remote HDF5 Read Paths](../internals/REMOTE_VFD_V2.md).

### SFTP Interrupt and Cleanup Behavior

The native SFTP backend keeps libssh2 sessions in nonblocking mode. Remote SSH/SFTP operations wait in short slices and
check DuckDB's query interrupt state between waits, so user interrupts can stop connection setup, authentication,
directory listing, file reads, and normal cleanup. The intentional exception is `getaddrinfo(...)`, which remains a
regular local resolver call to avoid adding resolver-specific machinery.

Cleanup is graceful by default: h5db tries to close SFTP handles, shut down the SFTP subsystem, send SSH disconnect, and
free the libssh2 session through the normal libssh2 APIs. Cleanup switches to bounded abortive behavior when either the
connection has already seen an unreliable remote-connection event or DuckDB reports the query context as interrupted.
Once that condition is observed, all cleanup steps in the current cleanup cascade share one 1s grace period. If graceful
cleanup still cannot make progress, h5db shuts down the socket transport and releases local ownership of the SFTP/session
objects rather than letting cleanup block the query indefinitely.

Windows CI currently skips this SQL glob test:

- `test/sql/glob/h5_glob_symlink.test`
  Contains full file-symlink glob coverage for `h5_tree`, `h5_ls`, and `h5_read`, including direct and globbed access to
  `test/data/glob_symlink/link_file.h5`. Windows CI showed that direct `H5Fopen` with HDF5 1.14.6 fails when the final
  path component is a file symlink, with HDF5 reporting an `H5FD__sec2_open` failure and `errno = 22`. The full test is
  marked `require notwindows`. Windows keeps narrower coverage in `test/sql/glob/h5_glob_symlink_windows.test`, which
  exercises symlink directories in intermediate path components without opening a file symlink as the final component.

---

## Working with Python Scripts

The project uses Python scripts to generate HDF5 test data files. Always use the virtual environment to ensure correct dependencies.

### Activating the Virtual Environment

```bash
# Activate venv (automatically sources .env)
source venv/bin/activate

# Your prompt will change to show (venv)
(venv) user@host:~/h5db$

# Deactivate when done
deactivate
```

### Running Python Scripts

**Method 1: With activated venv**

```bash
source venv/bin/activate
cd test/data
python create_rse_edge_cases.py
deactivate
```

**Method 2: Direct execution (recommended)**

```bash
# Run directly without activating venv
./venv/bin/python test/data/create_rse_edge_cases.py
```

**Method 3: From within test/data directory**

```bash
cd test/data
../../venv/bin/python create_rse_edge_cases.py
cd ../..
```

### Regenerating Test Data

If you modify a test data generation script or need to recreate test files:

```bash
# Regenerate the entire test-data tree
./test/data/generate_all_test_data.sh

# Or regenerate only the fixture touched by your change
cd test/data
../../venv/bin/python create_unsupported_types_test.py
cd ../..
```

`test/data/ensure_test_data.sh` only generates missing files. If you change a generator script, rerun that specific
generator or `generate_all_test_data.sh`.

### Installing Additional Python Packages

```bash
source venv/bin/activate
pip install <package-name>
deactivate
```

---

## Development Workflow

### Typical Development Cycle

1. **Make code changes** in `src/`
2. **Rebuild** the extension:
   ```bash
   source venv/bin/activate && make -j4
   ```
3. **Run tests** to verify:
   ```bash
   ./build/release/test/unittest "test/sql/*" "~test/sql/remote/*"
   ```
   ```bash
   # Optional: skip slow tests
   ./build/release/test/unittest "test/sql/*" "~test/sql/large/*" "~test/sql/remote/*"
   ```
4. **Interactive testing** with DuckDB CLI:
   ```bash
   ./build/release/duckdb
   D SELECT * FROM h5_read('test/data/simple.h5', '/integers');
   D SELECT * FROM h5_ls('test/data/links.h5');
   ```

### Code Formatting

The formatting targets delegate to DuckDB's formatter, which creates its own tool environment under
`duckdb/.cache/prepare-venv`
and installs missing formatter dependencies. The first run requires network access. The repo venv is used separately
for test data and SFTP tests.

**Optional pre-commit hook**:
```bash
./scripts/install-pre-commit-hook.sh    # Optional: auto-check on commit
```

**Before committing**:
```bash
# Check formatting (fast)
make format-check

# Auto-fix formatting issues
make format-fix
```

**What gets formatted:**
- C/C++ code: `clang-format` (uses DuckDB's .clang-format rules)
- Python code: `black`
- CMake files: `cmake-format`

**Pre-commit hook** (if installed):
- Automatically runs `make format-check` before each commit
- Blocks commits with formatting issues
- To bypass temporarily: `git commit --no-verify`
- If formatter setup fails, check the dependency-install output and network access

### Thread Safety Considerations

**IMPORTANT**: The HDF5 library is not thread-safe. To prevent race conditions and crashes when DuckDB parallelizes query execution, all HDF5 API calls are protected by a global mutex (`hdf5_global_mutex`).

**What this means for developers:**

1. **When adding new HDF5 function calls**, always protect them with the mutex:
   ```cpp
   // Lock for all HDF5 operations (not thread-safe)
   std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

   // Now safe to call HDF5 API
   hid_t file_id = H5Fopen(...);
   ```

2. **Protected locations** (current shape):
   - `h5_tree`: file opens, resumable namespace-iteration slices, object resolution, and any projected dataset metadata
     or attribute reads; the mutex is released before each produced batch is returned to DuckDB
   - `h5_ls`: file opens, group validation, immediate-child iteration, and any projected child metadata or attribute
     reads; scalar `h5_ls` reads its complete result while holding the mutex
   - `h5_read`: schema determination, dataset opens, and scan-time HDF5 reads
   - `h5_attributes`: attribute schema reads and attribute value reads

3. **Performance implications**:
   - The mutex serializes all HDF5 operations across threads
   - This prevents crashes but may reduce parallelism
   - This is necessary for correctness with the current HDF5 build

4. **Future optimizations** (if needed):
   - Use thread-safe HDF5 builds (requires specific compile flags)
   - Implement fine-grained locking per file handle
   - Consider read-write locks for concurrent reads

**Historical context**: A critical segmentation fault bug (BUG_UNION_ALL_SEGFAULT.md) was discovered when DuckDB created 12 threads for parallel execution of UNION ALL queries. The crash occurred in `H5C_protect` during parallel `H5ReadInit` calls. The global mutex fix resolved this issue.

### Adding New Tests

1. **Create test data** (if needed):
   ```bash
   # Create a Python script in test/data/
   vim test/data/create_mytest.py

   # Generate the HDF5 file
   ./venv/bin/python test/data/create_mytest.py
   ```

2. **Add test cases** to an existing `.test` file or create a new one:
   ```bash
   vim test/sql/mytest.test
   ```

3. **Run the new tests**:
   ```bash
   ./build/release/test/unittest "test/sql/mytest.test"
   ```

### Debugging

**Using the DuckDB CLI for interactive debugging**:

```bash
./build/release/duckdb

# Turn on query timing
D .timer on

# Run queries and inspect results
D SELECT * FROM h5_tree('test/data/simple.h5');
D SELECT * FROM h5_ls('test/data/links.h5');
D SELECT * FROM h5_read('test/data/simple.h5', '/integers');
```

**Debugging with GDB**:

```bash
# Build with debug symbols
source venv/bin/activate && make debug

# Run under GDB
gdb --args ./build/debug/test/unittest "test/sql/*" "~test/sql/remote/*"
```

**Viewing test file contents**:

```bash
# Check what's in an HDF5 file
./build/release/duckdb -c "SELECT * FROM h5_tree('test/data/simple.h5');"

# Check one group's immediate children
./build/release/duckdb -c "SELECT * FROM h5_ls('test/data/links.h5');"

# Check structure
./build/release/duckdb -c "SELECT path, type, dtype, shape FROM h5_tree('test/data/simple.h5');"
```

---

## Project Structure

```
h5db/
├── .env                      # Build environment configuration
├── Makefile                  # Main build file
├── CMakeLists.txt            # CMake configuration
├── vcpkg.json               # Dependency specification
├── extension_config.cmake   # Extension-specific CMake config
│
├── src/                     # Source code
│   ├── h5db_extension.cpp   # Extension entry point
│   ├── h5_read_table.cpp    # table h5_read implementation
│   ├── h5_read_scalar.cpp   # scalar h5_read implementation
│   ├── h5_read_shared.cpp   # shared h5_read dataset helpers
│   ├── h5_remote_backend.cpp # DuckDB-FS and SFTP remote backends
│   ├── h5_remote_vfd.cpp    # HDF5 remote VFD glue
│   ├── h5_sftp_secrets.cpp  # DuckDB TYPE sftp secret registration
│   ├── h5_attr.cpp          # h5_attr projected-attribute marker
│   ├── h5_tree.cpp          # h5_tree implementation
│   ├── h5_ls.cpp            # h5_ls table/scalar implementations
│   ├── h5_tree_shared.cpp   # shared h5_tree/h5_ls metadata helpers
│   ├── h5_attributes.cpp    # h5_attributes implementation
│   └── h5_common.cpp        # Shared HDF5 helpers
│
├── test/                    # Test suite
│   ├── sql/                 # SQLLogicTest files
│   └── data/                # Test data files
│       ├── *.h5             # HDF5 test files
│       └── *.py             # Data generation scripts
│
├── docs/                    # Documentation
│   ├── README.md            # Documentation index
│   ├── USER_GUIDE.md        # Practical user guide
│   ├── API.md               # Public API reference
│   ├── RSE_USAGE.md         # RSE user guide
│   ├── developer/           # Contributor docs
│   └── internals/           # Internal design notes
│
├── duckdb/                  # DuckDB submodule
├── extension-ci-tools/      # CI/CD tooling
└── venv/                    # Python virtual environment
```

### Key Files

- **`.env`**: Environment configuration (VCPKG path, build settings)
- **`src/h5_read_table.cpp`**: Table `h5_read`, run-encoded scanner, shared chunk-cache coordination, and multi-file
  scan wrapper
- **`src/h5_read_scalar.cpp`**: Scalar `h5_read`, including runtime-typed dataset materialization into `VARIANT`
- **`src/h5_read_shared.cpp`**: Dataset opening, contextual errors, checked sizing, and string decoding shared by both
  `h5_read` forms
- **`src/h5_read_table.cpp`** intentionally does not currently register DuckDB `get_partition_data`. An earlier
  partition-based implementation was removed because early-stopping plans could abandon a partition while still
  blocking shared cache progress. Reintroduce it only with a design that does not make cache progress depend on a
  local scan state returning after it has produced a chunk.
- **`src/h5_remote_backend.cpp`**: DuckDB-backed remote access plus `sftp://` backend
- **`src/h5_remote_vfd.cpp`**: HDF5 VFD integration for remote files
- **`src/h5_sftp_secrets.cpp`**: registration and validation for DuckDB `TYPE sftp` secrets
- **`src/h5_attr.cpp`**: `h5_attr(...)` projected-attribute marker registration
- **`src/h5_tree.cpp`**: recursive namespace listing
- **`src/h5_ls.cpp`**: immediate-child listing (`h5_ls` table and scalar forms)
- **`src/h5_tree_shared.cpp`**: shared row resolution, metadata, and projected-attribute helpers for `h5_tree`/`h5_ls`
- **`src/h5_attributes.cpp`**: Attribute reader
- **`src/h5_common.cpp`**: Shared HDF5 helpers
- **`test/sql/*.test`**: SQLLogicTest test files (regular)
- **`test/sql/remote/*.test`**: remote-only SQLLogicTests (auth, retries, redirects, transport faults)
- **`test/sql/large/*.test`**: large SQLLogicTests (slow)
- **`test/scripts/run_remote_tests.sh`**: rewritten remote HTTP suite harness
- **`test/scripts/run_sftp_tests.sh`**: rewritten remote SFTP suite harness
- **`test/scripts/run_sftp_interaction_tests.py`**: dedicated SFTP interaction/auth/cache harness
- **`test/scripts/sftp_test_server_lib.py`**: local rooted SFTP test server implementation
- **`vcpkg.json`**: Dependencies (`hdf5`, `libssh2`)

---

## Troubleshooting

### SWMR Notes

Observed SWMR open behavior can vary by HDF5 build/version. In this repo’s current setup (HDF5 1.14.6 via vcpkg),
opening with `H5F_ACC_SWMR_READ` succeeds even for files not written in SWMR mode. Other builds may reject SWMR opens
unless the file is marked by a SWMR writer, so don’t assume identical behavior across environments.

### Build Issues

**Problem**: CMake can't find HDF5
```
Solution: Ensure VCPKG_TOOLCHAIN_PATH is set correctly in .env:
  source venv/bin/activate
  echo $VCPKG_TOOLCHAIN_PATH
  # Should print: /path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
```

**Problem**: Build fails with "generator does not match"
```
Solution: Clean and rebuild:
  make clean
  source venv/bin/activate && make -j4
```

**Problem**: Undefined reference errors
```
Solution: HDF5 libraries may not be linked. Check CMakeLists.txt includes:
  target_link_libraries(...  ${HDF5_C_LIBRARIES})
```

### Test Issues

**Problem**: Tests fail with "File not found"
```
Solution: Ensure test data exists, then run tests from project root:
  cd /path/to/h5db
  ./test/data/ensure_test_data.sh
  ./build/release/test/unittest "test/sql/*" "~test/sql/remote/*"
```

**Problem**: Python script fails with "ModuleNotFoundError"
```
Solution: Use the venv Python:
  ./venv/bin/python test/data/script.py
  # NOT: python3 test/data/script.py
```

**Problem**: HDF5 test file is outdated
```
Solution: Regenerate all test data:
  ./test/data/generate_all_test_data.sh
```

### Environment Issues

**Problem**: Commands fail with "command not found"
```
Solution: Activate the virtual environment first:
  source venv/bin/activate
  make -j4
```

**Problem**: Formatter dependency installation fails
```
Solution: Check network access and the pip error from DuckDB's formatter environment, then retry:
  make format-check
```

**Problem**: VCPKG dependencies not found
```
Solution: Ensure vcpkg is bootstrapped and path is set:
  cd /path/to/vcpkg
  ./bootstrap-vcpkg.sh

  # Set the path using pwd
  export VCPKG_TOOLCHAIN_PATH=`pwd`/scripts/buildsystems/vcpkg.cmake

  # Verify it's set
  echo $VCPKG_TOOLCHAIN_PATH
```

---

## Quick Reference

### Common Commands

```bash
# Build
source venv/bin/activate && make -j4

# Test all suites
make test

# Test local SQLLogicTests only
./build/release/test/unittest "test/sql/*" "~test/sql/remote/*"

# Optional: skip slow tests
./build/release/test/unittest "test/sql/*" "~test/sql/large/*" "~test/sql/remote/*"

# Test rewritten remote HTTP suite
make test_remote_http

# Test rewritten remote SFTP suite + interaction harness
make test_remote_sftp

# Ensure test data exists (generate if missing)
./test/data/ensure_test_data.sh

# Test specific file
./build/release/test/unittest "test/sql/<testfile>.test"

# Run DuckDB CLI
./build/release/duckdb

# Run Python script
./venv/bin/python test/data/<script>.py

# Clean build
make clean

# Regenerate test data
./test/data/generate_all_test_data.sh
```

### Environment Variables

After `scripts/setup-dev-env.sh`, variables in `.env` are loaded when you activate the virtual environment:

- **`VCPKG_TOOLCHAIN_PATH`**: Path to vcpkg CMake toolchain
- **`GEN`**: Build generator (ninja for fast builds)

Example `.env` content:

```bash
export VCPKG_TOOLCHAIN_PATH=/home/yourusername/personal/vcpkg/scripts/buildsystems/vcpkg.cmake
export GEN=ninja
```

To set the path dynamically (from vcpkg directory):

```bash
cd /path/to/vcpkg
export VCPKG_TOOLCHAIN_PATH=`pwd`/scripts/buildsystems/vcpkg.cmake
```

**Note**: The `.env` activation hook is installed by `scripts/setup-dev-env.sh`; a manually created venv needs its
build variables exported separately.

### Build Targets

```bash
make              # Release build
make debug        # Debug build
make test         # Run tests (generates data if missing)
make clean        # Clean build artifacts
```

---

## Additional Resources

- **Main README**: `README.md` - Project overview and usage
- **API Reference**: `../API.md` - Complete function reference
- **RSE Documentation**: `../RSE_USAGE.md` - Run-Start Encoding guide

For questions or issues, please check existing documentation or open an issue on GitHub.
