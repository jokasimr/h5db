# H5DB: HDF5 DuckDB Extension - Implementation Plan

## Project Overview

Transform this DuckDB extension template into a fully functional HDF5 reader extension that enables DuckDB to query HDF5 files directly using SQL.

### Goals
- Enable reading HDF5 datasets as DuckDB tables
- Support common HDF5 data types and structures
- Integrate seamlessly with DuckDB's query engine
- Provide efficient data access with minimal memory overhead

## Technical Background

**HDF5 (Hierarchical Data Format 5)** is a file format and library designed for storing and managing large, complex datasets. It's widely used in scientific computing, machine learning, and data science.

Key HDF5 concepts:
- **Groups**: Directory-like containers (similar to folders)
- **Datasets**: Multidimensional arrays of data
- **Attributes**: Metadata attached to groups/datasets
- **Datatypes**: Integer, float, string, compound, etc.
- **Chunking**: Data storage in fixed-size blocks
- **Compression**: Optional compression (gzip, szip, etc.)

## Implementation Phases

### Phase 1: Project Bootstrap
**Objective**: Rename template and set up project structure

- [ ] Run `bootstrap-template.py` to rename from "quack" to "h5db"
- [ ] Update README.md with H5DB project description
- [ ] Update extension metadata (name, description, version)
- [ ] Verify build system works with renamed extension

### Phase 2: HDF5 Dependency Integration
**Objective**: Add HDF5 C library as a dependency

- [ ] Add HDF5 library to vcpkg.json (or find alternative dependency management)
- [ ] Update CMakeLists.txt to find and link HDF5
- [ ] Create simple test to verify HDF5 library is linked correctly
- [ ] Document HDF5 version requirements and compatibility

### Phase 3: Basic HDF5 File Reading
**Objective**: Implement basic file opening and metadata inspection

- [ ] Implement HDF5 file opening/closing with proper error handling
- [ ] Create `h5_tree(file_path)` function to list groups and datasets in an HDF5 file
  - Returns hierarchical structure with full paths
  - Shows dataset shapes and data types
- [ ] Implement dataset metadata inspection (shape, dtype, attributes)
- [ ] Add basic error handling for invalid files

### Phase 4: Table Function Implementation
**Objective**: Read HDF5 datasets as DuckDB tables

- [ ] Implement table-valued function: `h5_read(file_path, dataset_path, ...)`
  - Support single dataset: `h5_read('file.h5', '/dataset')`
  - Support nested groups: `h5_read('file.h5', '/group1/subgroup/dataset')`
  - Support multiple datasets with variadic arguments: `h5_read('file.h5', '/ds1', '/group/ds2', '/ds3')`
  - Multiple datasets are stacked horizontally (columns) - must have same length
- [ ] Map HDF5 datatypes to DuckDB types:
  - Signed integers: int8, int16, int32, int64
  - Unsigned integers: uint8, uint16, uint32, uint64
  - Floating point: float16, float32, float64
  - Strings: VARCHAR (both fixed and variable length)
  - Boolean: BOOLEAN
- [ ] Handle 1D datasets initially (produces single column with scalar type)
- [ ] Add multi-dimensional dataset support (N-D arrays)
  - Each dataset produces a single column with fixed-size array type
  - 2D dataset with shape [10, 3] → column with type `integer[3]` (10 rows)
  - 3D dataset with shape [10, 4, 2] → column with type `integer[4][2]` (10 rows)
  - First dimension is the row count, remaining dimensions become nested array types
- [ ] Implement basic data conversion from HDF5 to DuckDB vectors

### Phase 5: DuckDB Table Function Integration
**Objective**: Use DuckDB's table function API for proper integration

- [ ] Implement TableFunction interface for h5_read
- [ ] Support bind phase for schema detection
- [ ] Implement init phase for dataset opening
- [ ] Implement scan phase for data reading
- [ ] Add proper resource cleanup in destructor

### Phase 6: Additional Functions
**Objective**: Implement utility functions for HDF5 inspection

- [ ] Implement `h5_attributes(file_path, dataset_path)` function
  - Returns table with attribute names, types, and values
  - Support both dataset and group attributes
- [ ] Add comprehensive error messages for common issues
- [ ] Document all function signatures and usage examples

### Phase 7: Performance Optimization
**Objective**: Optimize data reading performance

- [ ] Implement chunked reading to avoid loading entire datasets
- [ ] Add support for HDF5 hyperslab selection
- [ ] Implement parallel chunk reading if beneficial
- [ ] Benchmark and optimize memory usage

### Phase 8: Advanced Features
**Objective**: Add advanced query capabilities (post-MVP)

- [ ] Support for compressed datasets (automatic decompression via HDF5 library)
- [ ] Filter pushdown to HDF5 layer (read only needed chunks)
- [ ] Optimize memory usage for large datasets
- [ ] Investigate zero-copy or memory-mapped access patterns

### Phase 9: Testing & Documentation
**Objective**: Comprehensive testing and user documentation

- [ ] Create test HDF5 files with various data types
- [ ] Write SQLLogicTest tests for all supported features
- [ ] Add unit tests for data type conversions
- [ ] Add integration tests for real-world HDF5 files
- [ ] Write comprehensive README with usage examples
- [ ] Create troubleshooting guide
- [ ] Document performance characteristics

### Phase 10: CI/CD & Distribution
**Objective**: Set up automated builds and distribution

- [ ] Configure GitHub Actions for multi-platform builds
- [ ] Ensure HDF5 dependency is available on all platforms
- [ ] Test extension loading on Linux, macOS, Windows
- [ ] Set up extension distribution mechanism
- [ ] Create release documentation

## Design Decisions

### 1. HDF5 Hierarchical Structure Handling
**Decision**: Start with explicit function calls using full paths, add ATTACH support later
- **Initial (MVP)**: Use `h5_read('file.h5', '/group1/group2/dataset')` with full paths
- **Future**: Add ATTACH capability so files don't need to be reopened: `ATTACH 'file.h5' AS h5file;` then `SELECT * FROM h5file.dataset`
- **Rationale**: Simpler implementation first, optimize for performance later

### 2. Multi-dimensional Array Handling
**Decision**: N-D datasets produce a single column with fixed-size array types
- 1D dataset with shape [N] → N rows, scalar type (e.g., INTEGER)
- 2D dataset with shape [N, 3] → N rows, type `INTEGER[3]`
- 3D dataset with shape [N, 4, 2] → N rows, type `INTEGER[4][2]`
- First dimension is always the row count
- Remaining dimensions become nested fixed-size array types
- **Rationale**: Natural mapping to DuckDB's array types, preserves dimensionality

### 3. Dataset Discovery
**Decision**: Start with explicit function calls, add auto-catalog later
- **Initial (MVP)**: Explicit calls only: `SELECT * FROM h5_read('file.h5', 'dataset')`
- **Future**: Add both explicit and ATTACH-based approaches
- **Rationale**: Simpler to implement and test, sufficient for initial use cases

### 4. Chunking Strategy
**Decision**: Optimize for full scans initially, plan for larger-than-memory support
- Start with reading full datasets optimized for sequential access
- Use HDF5's native chunking where beneficial
- Align with DuckDB's vector size (2048 elements) when possible
- **Future**: Implement streaming for datasets larger than available memory
- **Rationale**: Common use case is reading entire datasets; streaming can be added later

### 5. Compression Support
**Decision**: Support only HDF5 built-in compression filters
- Support: gzip, szip (if available in HDF5 build)
- Automatic decompression handled by HDF5 library
- Document unsupported filters with clear error messages
- **Rationale**: Reduces dependency complexity, covers most common use cases

### 6. Attribute Handling
**Decision**: Use dedicated function for attribute access
- Implement `h5_attributes(file_path, dataset_path)` table function
- Returns table with columns: attribute_name, type, value
- Support attributes on both datasets and groups
- **Rationale**: Clean separation between data and metadata access

### 7. String Handling
**Decision**: Use DuckDB VARCHAR for all HDF5 string types
- Map both fixed-length and variable-length HDF5 strings to VARCHAR
- Handle UTF-8 encoding by default
- Allocate and copy strings during HDF5→DuckDB conversion
- **Future**: Optimize with string pooling or zero-copy if needed
- **Rationale**: Simple, correct implementation; can optimize later based on profiling

## Future Enhancements

### 1. Parallel I/O
- HDF5 supports parallel I/O with MPI
- DuckDB has parallel query execution
- Investigate parallel HDF5 reads for large datasets
- Challenge: Thread safety and HDF5 library configuration
- Priority: Medium (performance optimization)

### 2. Memory Management for Large Datasets
- Large HDF5 datasets may not fit in memory
- DuckDB has streaming and out-of-core capabilities
- Implement proper streaming without loading entire datasets
- Monitor memory usage patterns during development
- Priority: High (enables larger datasets)

### 3. ATTACH Support
- Allow attaching HDF5 files as schemas
- Syntax: `ATTACH 'file.h5' AS h5file;` then `SELECT * FROM h5file.dataset`
- Avoids reopening files for each query
- Requires caching file handles and metadata
- Priority: Medium (usability improvement)

### 4. Cross-platform Binary Distribution
- HDF5 library availability varies across platforms
- Challenge: Static vs dynamic linking
- Ensure consistent behavior across Linux, macOS, Windows
- Consider: Bundle HDF5 library or require system installation
- Priority: High (for v1.0 release)

### 5. Version Compatibility
- HDF5 library versions (1.8.x, 1.10.x, 1.12.x, 1.14.x)
- File format versions
- Document supported versions, test compatibility
- Establish minimum version requirements
- Priority: Medium (documentation and testing)

### 6. Performance Benchmarking
- Benchmark against:
  - Native HDF5 C API access
  - Python h5py + pandas
  - Other HDF5 query tools
- Establish performance baselines
- Identify optimization opportunities
- Priority: Low (after MVP is functional)

### 7. Error Handling and User Experience
- HDF5 errors can be cryptic
- Provide clear, actionable error messages
- Handle corrupt files gracefully
- Provide helpful diagnostics for common issues
- Priority: High (critical for usability)

### 8. Extension Ecosystem Integration
- How does h5db interact with other DuckDB extensions?
- Potential synergies with spatial, parquet, or arrow extensions
- Consider interoperability patterns
- Priority: Low (exploration phase)

## Success Criteria

**Minimum Viable Product (MVP)**:
- Read HDF5 datasets from root and nested groups
- Support basic data types: integers (signed/unsigned), floats, strings, booleans
- Handle 1D datasets (N-D support can come later)
- Table functions: `h5_read('file.h5', '/dataset')` and `h5_tree('file.h5')`
- Works on Linux (initial platform target)
- Basic test coverage with SQLLogicTest

**v1.0 Release**:
- All common HDF5 data types supported (including float16)
- Multi-dimensional arrays as fixed-size array types
- Multiple dataset reading with variadic arguments
- Metadata access via `h5_attributes()` function
- Cross-platform support (Linux, macOS, Windows)
- Automatic decompression (gzip, szip)
- Comprehensive documentation with examples
- Performance competitive with native tools (h5py + pandas)

**Future Versions**:
- ATTACH support for persistent file handles
- Streaming for larger-than-memory datasets
- Filter and projection pushdown
- Parallel I/O optimization

## Dependencies

**Required**:
- HDF5 C library (>= 1.10.x recommended)
- DuckDB (v1.4.2, via submodule)
- OpenSSL (existing dependency)

**Build Tools**:
- CMake >= 3.15
- C++11 compatible compiler
- VCPKG (for dependency management)

**Optional**:
- HDF5 compression plugins (szip, blosc, etc.)
- Testing: h5py or similar for generating test files

## Next Steps

1. ✅ Review plan and make design decisions
2. **Begin Phase 1**: Bootstrap the template (rename from "quack" to "h5db")
3. **Phase 2**: Research and integrate HDF5 library dependency
4. **Phase 3**: Create proof-of-concept for basic file reading
5. **Phase 4-6**: Implement core functionality (h5_read, h5_tree, h5_attributes)
6. Iterate based on testing and feedback

## API Summary

Once implemented, the extension will provide:

```sql
-- List all datasets and groups in an HDF5 file
SELECT * FROM h5_tree('data.h5');

-- Read a single dataset
SELECT * FROM h5_read('data.h5', '/dataset_name');

-- Read dataset from nested group
SELECT * FROM h5_read('data.h5', '/group1/subgroup/dataset');

-- Read multiple datasets (horizontal stacking)
SELECT * FROM h5_read('data.h5', '/dataset1', '/dataset2', '/group/dataset3');

-- Get attributes for a dataset or group
SELECT * FROM h5_attributes('data.h5', '/dataset_name');
```

**Data type mappings:**
- HDF5 integers (int8, int16, int32, int64, uint8, uint16, uint32, uint64) → DuckDB INTEGER types
- HDF5 floats (float16, float32, float64) → DuckDB FLOAT/DOUBLE
- HDF5 strings (fixed/variable length) → DuckDB VARCHAR
- HDF5 booleans → DuckDB BOOLEAN
- HDF5 N-D arrays → DuckDB fixed-size arrays (e.g., INTEGER[3], DOUBLE[4][2])
