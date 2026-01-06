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

- **C++ compiler**: GCC 9+ or Clang 10+
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

**Update the .env file**:

The project includes a `.env` file that needs to be configured with your vcpkg path:

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

The project includes a Python virtual environment for running test data generation scripts:

```bash
# The venv is already created, but if you need to recreate it:
python3 -m venv venv
source venv/bin/activate
pip install h5py numpy
deactivate
```

**Note**: The venv is configured to automatically source `.env` when activated.

---

## Building the Project

### Quick Start

The simplest way to build:

```bash
# Activate virtual environment (includes all tools and environment variables)
source venv/bin/activate

# Build the project (uses ninja and ccache automatically)
make -j$(nproc)
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

### Build Variants

```bash
# Release build (default, optimized)
source venv/bin/activate && make

# Debug build (with debug symbols, no optimization)
source venv/bin/activate && make debug

# Release with debug info
source venv/bin/activate && make reldebug
```

### First Build vs. Incremental Builds

**First build**:
- Downloads and compiles all dependencies (HDF5, zlib, etc.)
- Compiles DuckDB and all extensions
- Takes ~10-15 minutes with ninja

**Incremental builds** (after making changes):
- Only recompiles changed files (thanks to ccache)
- Takes 10-60 seconds depending on changes
- Extension-only changes: ~10-30 seconds

### Clean Build

If you need to start fresh:

```bash
# Clean all build artifacts
make clean

# Then rebuild
source venv/bin/activate && make -j$(nproc)
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
```

### Running All Tests

```bash
# Activate environment and run all tests
source venv/bin/activate && ./build/release/test/unittest "test/*"
```

### Running Specific Tests

```bash
# Run a specific test file
source venv/bin/activate && ./build/release/test/unittest "test/sql/<testfile>.test"
```

### Running Individual Test Cases

You can filter tests by pattern:

```bash
# Run tests matching a pattern
source venv/bin/activate && ./build/release/test/unittest "test/sql/<testfile>.test" -testcase="*pattern*"
```

### Using the Makefile Test Target

The Makefile provides a convenient test target:

```bash
# This internally runs: ./build/release/test/unittest "test/*"
make test
```

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
# Regenerate all test data
cd test/data

# Main test files
../../venv/bin/python create_attrs_test.py
../../venv/bin/python create_rse_edge_cases.py

cd ../..
```

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
   source venv/bin/activate && make -j$(nproc)
   ```
3. **Run tests** to verify:
   ```bash
   source venv/bin/activate && ./build/release/test/unittest "test/*"
   ```
4. **Interactive testing** with DuckDB CLI:
   ```bash
   ./build/release/duckdb
   D SELECT * FROM h5_read('test/data/simple.h5', '/integers');
   ```

### Code Formatting

The project uses automated code formatting to maintain consistency.

**One-time setup** (already configured):
```bash
./scripts/setup-dev-env.sh              # Install formatting tools in venv
./scripts/install-pre-commit-hook.sh    # Optional: auto-check on commit
```

**Before committing**:
```bash
source venv/bin/activate

# Check formatting (fast)
make format-check

# Auto-fix formatting issues
make format

# Optional: run static analysis (slower)
make tidy-check
```

**What gets formatted:**
- C/C++ code: `clang-format` (uses DuckDB's .clang-format rules)
- Python code: `black`
- CMake files: `cmake-format`

**Pre-commit hook** (if installed):
- Automatically runs `make format-check` before each commit
- Blocks commits with formatting issues
- To bypass temporarily: `git commit --no-verify`

### Thread Safety Considerations

**IMPORTANT**: The HDF5 library is not thread-safe. To prevent race conditions and crashes when DuckDB parallelizes query execution, all HDF5 API calls are protected by a global mutex (`hdf5_global_mutex`).

**What this means for developers:**

1. **When adding new HDF5 function calls**, always protect them with the mutex:
   ```cpp
   // Lock for all HDF5 operations (not thread-safe)
   std::lock_guard<std::mutex> lock(hdf5_global_mutex);

   // Now safe to call HDF5 API
   hid_t file_id = H5Fopen(...);
   ```

2. **Protected locations** (as of 2024-12-22):
   - `H5TreeInit`: File opening and tree traversal
   - `H5ReadBind`: Schema determination
   - `H5ReadInit`: Dataset opening during initialization
   - `H5ReadScan`: Data reading (including RSE expansion)
   - `H5AttributesBind`: Attribute metadata reading
   - `H5AttributesScan`: Attribute value reading

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
   source venv/bin/activate && ./build/release/test/unittest "test/sql/mytest.test"
   ```

### Debugging

**Using the DuckDB CLI for interactive debugging**:

```bash
./build/release/duckdb

# Turn on profiling
D .timer on

# Run queries and inspect results
D SELECT * FROM h5_tree('test/data/simple.h5');
D SELECT * FROM h5_read('test/data/simple.h5', '/integers');
```

**Debugging with GDB**:

```bash
# Build with debug symbols
source venv/bin/activate && make debug

# Run under GDB
gdb --args ./build/debug/test/unittest "test/*"
```

**Viewing test file contents**:

```bash
# Check what's in an HDF5 file
./build/release/duckdb -c "SELECT * FROM h5_tree('test/data/simple.h5');"

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
│   └── h5_functions.cpp     # Main implementation
│
├── test/                    # Test suite
│   ├── sql/                 # SQLLogicTest files
│   └── data/                # Test data files
│       ├── *.h5             # HDF5 test files
│       └── *.py             # Data generation scripts
│
├── docs/                    # Documentation
│   ├── DEVELOPER.md         # This file
│   └── UPDATING.md
│
├── benchmark/               # Performance benchmarks
│   ├── generate_benchmark_data.py
│   ├── run_benchmark_final.sql
│   └── run_multiple.sh
│
├── duckdb/                  # DuckDB submodule
├── extension-ci-tools/      # CI/CD tooling
└── venv/                    # Python virtual environment
```

### Key Files

- **`.env`**: Environment configuration (VCPKG path, build settings)
- **`src/h5_functions.cpp`**: Core HDF5 reading logic, RSE scanner
- **`test/sql/*.test`**: SQLLogicTest test files
- **`vcpkg.json`**: Dependencies (HDF5, etc.)

---

## Troubleshooting

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
  source venv/bin/activate && make -j$(nproc)
```

**Problem**: Undefined reference errors
```
Solution: HDF5 libraries may not be linked. Check CMakeLists.txt includes:
  target_link_libraries(...  ${HDF5_C_LIBRARIES})
```

### Test Issues

**Problem**: Tests fail with "File not found"
```
Solution: Tests expect to be run from project root:
  cd /path/to/h5db
  source venv/bin/activate && ./build/release/test/unittest "test/*"
```

**Problem**: Python script fails with "ModuleNotFoundError"
```
Solution: Use the venv Python:
  ./venv/bin/python test/data/script.py
  # NOT: python3 test/data/script.py
```

**Problem**: HDF5 test file is outdated
```
Solution: Regenerate test data:
  cd test/data
  ../../venv/bin/python create_rse_edge_cases.py
  cd ../..
```

### Environment Issues

**Problem**: Commands fail with "command not found"
```
Solution: Activate the virtual environment first:
  source venv/bin/activate
  make -j$(nproc)
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
source venv/bin/activate && make -j$(nproc)

# Test everything
source venv/bin/activate && ./build/release/test/unittest "test/*"

# Test specific file
source venv/bin/activate && ./build/release/test/unittest "test/sql/<testfile>.test"

# Run DuckDB CLI
./build/release/duckdb

# Run Python script
./venv/bin/python test/data/<script>.py

# Clean build
make clean

# Regenerate test data (example)
cd test/data
../../venv/bin/python <create_script>.py
cd ../..
```

### Environment Variables

These are set in `.env` and automatically loaded when you activate the virtual environment:

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

**Note**: When you run `source venv/bin/activate`, the `.env` file is automatically sourced, so you get both the Python environment and all build variables.

### Build Targets

```bash
make              # Release build
make debug        # Debug build
make test         # Run tests
make clean        # Clean build artifacts
```

---

## Additional Resources

- **Main README**: `README.md` - Project overview and usage
- **API Reference**: `API.md` - Complete function reference
- **RSE Documentation**: `RSE_USAGE.md` - Run-Start Encoding guide
- **Project Status**: `STATUS.md` - Current implementation status
- **CLAUDE.md**: `CLAUDE.md` - Instructions for AI agents working on this project

For questions or issues, please check existing documentation or open an issue on GitHub.
