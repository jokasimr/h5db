# Remote HDF5 Reads in h5db: Viable Design, Alternatives, and Plan

## Scope
This note proposes a viable implementation for this repository's current dependency versions:

- DuckDB submodule: v1.4.x line (repo includes tags up to v1.4.4)
- HDF5 from vcpkg in this repo: 1.14.6 (already documented in `docs/DEVELOPER.md`)

Goal: support `h5_read`, `h5_tree`, `h5_attributes` on remote URLs (HTTP/S, and other `httpfs`-handled schemes) while preserving local-file performance.

---

## What I Verified

### API compatibility checks
- DuckDB in this repo exposes extension autoload APIs through `duckdb/main/extension_helper.hpp`.
- File-system access for extension code should use `FileSystem::GetFileSystem(...)` on an active DuckDB context/database instance.
- HDF5 1.14.6 exposes custom VFD interfaces via `H5FDdevelop.h`; `H5FD_class_t` must be implemented with the exact callback signatures and field ordering for this version.

### Current h5db open path
- `H5FileHandle` currently always uses `H5Fopen(..., fapl)` with file locking disabled, no custom VFD.
- All HDF5 calls are serialized by global `std::recursive_mutex hdf5_global_mutex`.

---

## Viable VFD-Based Design (DuckDB httpfs + HDF5 1.14.6)

## Design principles
1. Keep local path behavior unchanged.
2. Route only remote paths through a custom read-only HDF5 VFD.
3. Use DuckDB `httpfs` through `ExtensionHelper::AutoLoadExtension(...)` and `FileSystem::GetFileSystem(...)`.
4. Keep the existing h5db global HDF5 mutex behavior unchanged initially.

## High-level flow
1. In `H5FileHandle` constructor:
   - Detect remote path by scheme (`http://`, `https://`, and optionally `s3://`, `r2://`, `gcs://`, etc.).
   - For remote paths only:
     - Build a VFD FAPL with a small driver-info struct containing context handle(s).
     - Register custom VFD once (`std::call_once`).
     - `H5Pset_driver(fapl, duckdb_vfd_id, driver_info)`.
2. HDF5 invokes custom VFD callbacks (`open/read/...`).
3. VFD callbacks use DuckDB FS APIs to open and read ranges from remote object.

## Required code structure

### 1) New files
- `src/include/h5_remote_vfd.hpp`
- `src/h5_remote_vfd.cpp`

### 2) Extend `H5FileHandle` API
Current constructor:
```cpp
H5FileHandle(const char *filename, unsigned flags, bool swmr)
```
Proposed overload:
```cpp
H5FileHandle(ClientContext *context, const char *filename, unsigned flags, bool swmr)
```
- Existing call sites with `ClientContext &context` should pass `&context`.
- Keep old overload for places without context if needed (local only).

### 3) Driver info plumbing (important)
VFD callbacks need access to DuckDB context/DB for `httpfs` and FS resolution.
Use HDF5 driver-info callbacks in `H5FD_class_t`:
- `fapl_size`
- `fapl_copy`
- `fapl_free`

Example driver-info payload:
```cpp
struct DuckDBVFDConfig {
    duckdb::ClientContext *context; // non-owning, query-lifetime
};
```

In `open` callback:
- Extract config via `H5Pget_driver_info(fapl_id)`.
- `ExtensionHelper::AutoLoadExtension(*config->context, "httpfs")`.
- `auto &fs = FileSystem::GetFileSystem(*config->context);`
- open file with `FileFlags::FILE_FLAGS_READ | FILE_FLAGS_NULL_IF_NOT_EXISTS`.

### 4) Implement H5FD 1.14.6-compatible class
Use `H5FDdevelop.h` types and fill `H5FD_class_t` in correct order for 1.14.x.
At minimum, implement:
- `open`, `close`, `cmp`, `query`
- `get_eoa`, `set_eoa`, `get_eof`
- `read`
- `truncate` (no-op for read-only)
- optionally `lock`/`unlock` as no-op success for read-only

And initialize `H5FD_t` base fields in returned file object.

### 5) Read semantics
- Use random access reads (`Read(buffer, size, offset)`) only.
- Enforce bounds checks (`addr + size <= eof`).
- Map FS exceptions to `-1` so HDF5 reports IO failure.

### 6) Scheme handling
Initial remote detection should match DuckDB/httpfs-handled schemes, not only HTTP(S):
- `http://`, `https://`, optionally `s3://`, `s3a://`, `s3n://`, `r2://`, `gcs://`, `gs://`, `hf://`.

---

## Non-Remote Performance Impact

If implemented with strict remote gating, expected impact on local reads is negligible:

- Additional work on local paths: a single scheme check at file-open time.
- No change in scan loops (`H5ReadScan`, caching logic, RSE logic) for local files.
- No extra `httpfs` autoload attempts for local files.

Potential local regressions to avoid:
1. Do not register/set custom VFD for local files.
2. Do not alter existing `fapl` options for local paths.
3. Do not add extra synchronization beyond current global mutex.

Conclusion for local performance: no meaningful throughput regression expected when remote path branch is not taken.

---

## Remote Performance Characteristics (Expected)

With custom VFD + DuckDB FS:
- Pros:
  - No full upfront download required.
  - Uses DuckDB `httpfs` configuration/secrets/retries.
  - Works with existing h5db scan/pushdown logic.
- Cons:
  - HDF5 metadata/data access pattern can trigger many small random reads.
  - Current global HDF5 mutex serializes HDF5 calls, reducing concurrency benefits.

This means remote performance will be workload-dependent:
- Good for selective reads / moderate file sizes.
- Potentially poor for high-latency object stores with highly fragmented access.

---

## Alternatives Investigated

## Alternative A: Stage remote file to local temp, then open normally
Implementation idea:
1. Detect remote path.
2. Use DuckDB `FileSystem` (with `httpfs`) to stream-copy remote object to a temp local file.
3. Open local temp file with standard HDF5 driver.

Pros:
- Very low implementation risk.
- Reuses existing, well-tested local HDF5 path.
- Avoids custom VFD complexity.

Cons:
- Full file download before first row.
- Extra disk usage and startup latency on large files.

Best use:
- MVP with strongest correctness and predictable behavior.

## Alternative B: HDF5 ROS3 VFD
Pros:
- Built-in remote VFD for S3-like reads.

Cons:
- Not based on DuckDB `httpfs`/secrets/settings.
- Narrower protocol/auth integration for this extension.

Best use:
- Not recommended for h5db if requirement is to use DuckDB `httpfs` consistently.

## Alternative C: Hybrid strategy
- Start with A (staging) for immediate support.
- Add custom VFD (main design above) behind setting/flag.

Pros:
- Fast delivery + path to better remote behavior.

Cons:
- Two code paths to maintain.

---

## Recommended Approach

Recommendation: **implement the custom DuckDB-backed HDF5 VFD**, with a safety fallback option to staged download if needed during rollout.

Why:
- Meets requirement to use DuckDB `httpfs` similarly to other extensions.
- Avoids mandatory full downloads.
- Keeps non-remote path effectively unchanged.

---

## Implementation Plan

1. Add `h5_remote_vfd` module and CMake wiring.
2. Implement HDF5 1.14.6-compatible VFD class with correct `H5FD_class_t` callbacks.
3. Add context-aware `H5FileHandle` overload and route table-function call sites (`h5_read`, `h5_tree`, `h5_attributes`) to it.
4. Apply remote-scheme detection + remote-only driver selection.
5. Add robust error messages that preserve current behavior for local files.
6. Add tests:
   - Unit/integration tests using local HTTP server (or pre-signed URL fixture) for `h5_tree`, `h5_read`, `h5_attributes`.
   - Verify `httpfs` autoload path works.
   - Verify local-path baseline tests unchanged.
7. Optional: add setting `h5db_remote_mode = 'vfd'|'stage'` for controlled rollout.

---

## Validation Checklist

Functional:
- `h5_tree('https://...')` returns expected structure.
- `h5_read('https://...')` works for numeric, string, multidim, scalar datasets.
- `h5_attributes('https://...')` works for scalar and array attrs.
- SWMR flag behavior unchanged for local files; defined behavior for remote files documented.

Compatibility:
- Builds against current DuckDB submodule and HDF5 1.14.6 headers.
- No API usage of removed/absent DuckDB functions.

Performance:
- Local read micro-benchmark before/after within noise range.
- Remote benchmark documented for representative files (small, medium, large; sequential vs sparse queries).

---

## Risks and Mitigations

1. **VFD ABI mistakes**
   - Mitigation: use `H5FDdevelop.h` exact signatures and callback ordering.
2. **Context lifetime issues in driver_info**
   - Mitigation: keep VFD use within query lifetime; validate no handle escapes query scope.
3. **Remote random-read overhead**
   - Mitigation: rely on DuckDB/httpfs caching; consider staged fallback mode.
4. **Global mutex limits parallel remote throughput**
   - Mitigation: accept for correctness initially; revisit locking granularity later.

---

## Practical Conclusion
- A corrected VFD implementation is viable and should not hurt non-remote performance when gated by path scheme.
- A staged-download fallback is the main credible alternative when prioritizing delivery safety over remote random-read efficiency.
