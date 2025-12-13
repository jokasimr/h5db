# Phase 2: HDF5 Dependency Integration

This document details the implementation plan for integrating the HDF5 C library into the h5db DuckDB extension.

## Overview

Add HDF5 as a dependency using vcpkg, configure the build system to link it, and create a simple verification test.

## Tasks

### Task 1: Update vcpkg.json

**Current state:**
```json
{
    "dependencies": [
        "openssl"
    ],
    ...
}
```

**Required change:**
```json
{
    "dependencies": [
        "openssl",
        "hdf5"
    ],
    ...
}
```

**Alternative with features (if needed):**
```json
{
    "dependencies": [
        "openssl",
        {
            "name": "hdf5",
            "features": ["cpp", "zlib"]
        }
    ],
    ...
}
```

Common HDF5 features:
- `cpp`: C++ API (we only need C API, so skip this)
- `zlib`: Built-in compression support (recommended)
- `szip`: Additional compression (optional)
- `parallel`: MPI parallel I/O (not needed for MVP)
- `tools`: Command-line tools (not needed)

**Recommendation:** Start with just `"hdf5"` (no features) to minimize dependencies.

### Task 2: Update CMakeLists.txt

**Add find_package call:**
```cmake
# After the OpenSSL find_package
find_package(HDF5 REQUIRED COMPONENTS C)
```

Note: We specify `COMPONENTS C` to use only the C API (not C++ or Fortran).

**Update include directories (if needed):**
```cmake
# After project definition
include_directories(src/include)
include_directories(${HDF5_INCLUDE_DIRS})  # Add this if needed
```

**Link HDF5 libraries:**
```cmake
# Update target_link_libraries for both targets
target_link_libraries(${EXTENSION_NAME}
    OpenSSL::SSL
    OpenSSL::Crypto
    ${HDF5_C_LIBRARIES}
)

target_link_libraries(${LOADABLE_EXTENSION_NAME}
    OpenSSL::SSL
    OpenSSL::Crypto
    ${HDF5_C_LIBRARIES}
)
```

**Alternative (if CMake provides HDF5 targets):**
```cmake
target_link_libraries(${EXTENSION_NAME}
    OpenSSL::SSL
    OpenSSL::Crypto
    hdf5::hdf5
)
```

### Task 3: Setup VCPKG (First-time setup)

Since the build warned that vcpkg.json exists but vcpkg wasn't used, we need to set it up:

**Option A: Use system vcpkg (Recommended)**
```bash
# Clone vcpkg (one-time)
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh

# Export the toolchain path
export VCPKG_TOOLCHAIN_PATH=$(pwd)/scripts/buildsystems/vcpkg.cmake

# Add to .bashrc or .zshrc for persistence
echo "export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake" >> ~/.bashrc
```

**Option B: Use DuckDB's extension-ci-tools vcpkg**

The extension already has vcpkg configuration pointing to `./extension-ci-tools/vcpkg_ports`. This should work if properly configured.

**Build with vcpkg:**
```bash
# With vcpkg toolchain
cmake -DCMAKE_TOOLCHAIN_FILE=$VCPKG_TOOLCHAIN_PATH \
      -DEXTENSION_STATIC_BUILD=1 \
      -DDUCKDB_EXTENSION_CONFIGS='path/to/extension_config.cmake' \
      -S "./duckdb/" -B build/release

# Or use the Makefile which should pick it up automatically
GEN=ninja make
```

### Task 4: Create HDF5 Verification Test

Add a simple function to verify HDF5 is linked correctly.

**Update src/h5db_extension.cpp:**

```cpp
#include <hdf5.h>

// Add new verification function
inline void H5dbVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
        unsigned majnum, minnum, relnum;
        H5get_libversion(&majnum, &minnum, &relnum);

        std::string version = "HDF5 version " +
                              std::to_string(majnum) + "." +
                              std::to_string(minnum) + "." +
                              std::to_string(relnum);
        return StringVector::AddString(result, version);
    });
}

// Register in LoadInternal
static void LoadInternal(ExtensionLoader &loader) {
    // ... existing functions ...

    // Add HDF5 version check function
    auto h5db_version_function = ScalarFunction("h5db_version", {LogicalType::VARCHAR},
                                                 LogicalType::VARCHAR, H5dbVersionScalarFun);
    loader.RegisterFunction(h5db_version_function);
}
```

### Task 5: Update Tests

**Update test/sql/h5db.test:**

```sql
# Remove old quack tests, add HDF5 verification
statement ok
SELECT h5db_version('test');

# Verify the function returns something containing "HDF5"
query I
SELECT h5db_version('test') LIKE '%HDF5%';
----
true
```

### Task 6: Documentation

**Update README.md build dependencies:**

```markdown
### Dependencies

- CMake >= 3.5
- C++11 compatible compiler
- HDF5 C library (>= 1.10.x)
- OpenSSL
- VCPKG (for dependency management)

#### VCPKG Setup

```bash
# Install vcpkg
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=$(pwd)/scripts/buildsystems/vcpkg.cmake

# Dependencies will be installed automatically during build
```
```

## Implementation Order

1. ✅ Research HDF5 integration options
2. ⬜ Setup VCPKG if not already configured
3. ⬜ Update vcpkg.json to add HDF5 dependency
4. ⬜ Update CMakeLists.txt to find and link HDF5
5. ⬜ Add h5db_version() function to verify HDF5 linking
6. ⬜ Build and test
7. ⬜ Update tests to verify HDF5 is working
8. ⬜ Document HDF5 version requirements

## Expected Challenges

### Challenge 1: HDF5 CMake Variables

Different HDF5 installations use different CMake variable names:
- `${HDF5_C_LIBRARIES}` vs `hdf5::hdf5`
- `${HDF5_INCLUDE_DIRS}` vs automatic via target

**Solution:** Try modern target-based linking first (`hdf5::hdf5`), fall back to variables if needed.

### Challenge 2: VCPKG Setup Complexity

Users may not have vcpkg configured properly.

**Solution:**
- Provide clear setup instructions
- Consider alternative: Document system package installation as fallback
  ```bash
  # Ubuntu/Debian
  sudo apt-get install libhdf5-dev

  # macOS
  brew install hdf5

  # Fedora
  sudo dnf install hdf5-devel
  ```

### Challenge 3: HDF5 Version Compatibility

Different HDF5 versions (1.8.x, 1.10.x, 1.12.x, 1.14.x) may have API differences.

**Solution:**
- Target HDF5 1.10.x as minimum (widely available, stable API)
- Test with multiple versions in CI/CD
- Use `H5_VERSION_GE` macros for version-specific code if needed:
  ```cpp
  #if H5_VERSION_GE(1,10,0)
      // Code for HDF5 1.10+
  #else
      // Fallback for older versions
  #endif
  ```

### Challenge 4: Static vs Dynamic Linking

vcpkg may default to static or dynamic linking depending on configuration.

**Solution:**
- Test both scenarios
- Document any platform-specific linking requirements
- May need to set vcpkg triplet: `export VCPKG_DEFAULT_TRIPLET=x64-linux-dynamic`

## Validation Criteria

Phase 2 is complete when:

- [ ] vcpkg.json includes hdf5 dependency
- [ ] CMakeLists.txt successfully finds and links HDF5
- [ ] Build completes without HDF5-related errors
- [ ] `h5db_version()` function returns HDF5 library version
- [ ] Tests pass with HDF5 verification
- [ ] Documentation updated with dependency info
- [ ] Works on at least one platform (Linux/macOS)

## Next Steps

After Phase 2 completion, proceed to:
- **Phase 3:** Basic HDF5 file reading and metadata inspection
- Implement file opening/closing with error handling
- Create h5_tree() function to list datasets

## References

- [HDF5 C API Reference](https://docs.hdfgroup.org/hdf5/develop/_r_m.html)
- [vcpkg HDF5 Port](https://github.com/microsoft/vcpkg/tree/master/ports/hdf5)
- [CMake FindHDF5](https://cmake.org/cmake/help/latest/module/FindHDF5.html)
- [DuckDB Extension Template](https://github.com/duckdb/extension-template)
