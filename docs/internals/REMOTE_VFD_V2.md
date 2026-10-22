# Remote HDF5 Read Paths

This note describes the implementation targeting DuckDB v2.0.0. The `V2` filename refers to an earlier h5db VFD
redesign, not the DuckDB release number.

Relevant sources:

- [h5_remote_vfd.cpp](../../src/h5_remote_vfd.cpp): HDF5 callbacks and small-read cache
- [h5_remote_backend.cpp](../../src/h5_remote_backend.cpp): DuckDB filesystem and native SFTP backends
- [remote_external_cache.test](../../test/sql/remote/remote_external_cache.test): HTTP cache coverage
- [run_sftp_interaction_tests.py](../../test/scripts/run_sftp_interaction_tests.py): SFTP cache and transport coverage

## Read Policy

The HDF5 VFD delegates transport operations to `H5RemoteBackend`. Remote files must remain unchanged while open;
cache validation does not provide a snapshot of a changing file. Remote opens omit `H5F_ACC_SWMR_READ`.

| HDF5 request | VFD behavior |
| --- | --- |
| Metadata, or raw data smaller than 30,000 bytes | Read through aligned 30,000-byte blocks in a per-file LRU |
| Raw data of at least 30,000 bytes | Pass the requested range to the backend |

The small-read LRU retains up to 100 copied blocks per open file. Blocks are loaded through the backend's cached
read and clipped at EOF. They are ordinary byte vectors, not pinned DuckDB cache handles.

All reads use the backend's cache-aware path. There is no per-query byte cutoff or separate direct-read handle.
When caching is disabled or validation prevents reuse, the backend reads without retaining data in the shared cache.

### Memory and disk limits

DuckDB's buffer manager accounts for cache payloads and evicts unpinned blocks under memory pressure. By default,
`external_file_cache_spill=false`: evicted payloads are discarded and must be fetched again if needed.

With spilling enabled, eligible remote blocks are written to temporary storage on eviction. There is no separate
external-cache disk quota; they share `max_temp_directory_size` with other temporary data. Its default is 90% of
available disk space. Hitting the limit can fail the query rather than switching to discarding blocks.

The DuckDB caching handle pins every block in an individual read's returned buffer group. Splitting a read into
cache blocks enables parallel fetching but does not make an arbitrarily large individual read stream within bounded
memory. A scan of a huge file can still use bounded memory by releasing each read's buffer group before reading more.

### Why coalesce small reads?

HDF5 can issue one VFD read per small dataset chunk even when SQL requests a large scan. Local fixtures contain
chunk payloads of 200, 3,072, and 8,192 bytes. Spatially adjacent reads share the VFD blocks, reducing network round
trips. Large raw requests bypass this LRU.

The 30,000-byte block size matches libssh2's maximum SFTP READ payload. The previous 30 KiB size was 30,720 bytes,
which could require an extra 720-byte return from libssh2. Libssh2 also performs read-ahead, so its successful read
calls are not a count of wire requests.

## DuckDB Filesystem Backend

HTTP, S3, and other DuckDB-backed remote schemes use `CachingFileSystem::Get(context)`. Handles use
`FILE_FLAGS_READ | FILE_FLAGS_DIRECT_IO` and prefer `RequestSizing::BY_READER`.

Cached reads use DuckDB 2.0's buffer groups and copy their contents into the caller's buffer. Under default cache
settings:

- ranges are rounded to the 4 KiB minimum block size;
- files no larger than the 2 MiB maximum block size are fetched whole;
- larger missing ranges are split into blocks of at most 2 MiB and fetched through DuckDB's async executor.

The `external_file_cache_request_sizing` setting can override the reader's preference. Cache validation, expiry,
eviction, and spilling are handled by DuckDB.

The same handle is used throughout large scans. When caching is disabled or validation prevents reuse, DuckDB can
still split large reads into parallel range requests without retaining the returned data in the shared cache.

`httpfs` metadata caching and the external file cache have different roles: metadata caching can avoid repeated
`HEAD` requests; the external file cache retains body ranges. A warm body cache alone does not guarantee zero
network operations when validation is enabled.

## Native SFTP Backend

SFTP uses `ExternalFileCache` directly. DuckDB's built-in remote-scheme classification does not include `sftp://`,
so h5db explicitly selects remote maximum block size, validation, and spill policy. `cache_local_files` does not
control this path. The remote minimum block size and request-sizing grid are not applied to SFTP reads.

`AcquireBlocks` provides shared ranges. h5db pins existing blocks or synchronously loads missing blocks using
DuckDB's buffer manager. Cached ranges appear in `duckdb_external_file_cache()`, and eligible blocks can spill when
`external_file_cache_spill` is enabled. Changing the maximum block size preserves reusable existing ranges.

Native SFTP file access is serialized under `hdf5_global_mutex`; the same libssh2 session must not be used
concurrently. The transport uses nonblocking libssh2 calls with interruptible socket waits and enables
`TCP_NODELAY`. See the developer guide for [interrupt and cleanup behavior](../developer/DEVELOPER.md#sftp-interrupt-and-cleanup-behavior).

### Validation and connection lifetime

- Default validation opens and stats the file. Cached data is invalidated when size or modification time changes.
- DuckDB's conservative mtime age check applies on open: matching modification times must be more than ten seconds
  old. Freshly fetched ranges can still be reused within an open handle.
- Without a usable mtime, validated reads bypass the shared cache; the per-file VFD LRU still works.
- `NO_VALIDATION` permits a warm cache to serve data without opening an SFTP connection. This accepts stale data if
  the remote file changes.
- Connections are shared within a query and released when the query and its retained HDF5 handles finish. A later
  query requiring validation establishes another connection, even if its body ranges are cached.

## Errors and Measurement

The VFD catches C++ exceptions at HDF5 callback boundaries and preserves the error text and interruption status in
thread-local state. Higher-level file and dataset operations consume that state to report the underlying failure.
Filesystem open failures must propagate as exceptions rather than being reduced to null handles.

For HTTP request counts and payloads, use `duckdb_logs_parsed('HTTP')`. For SFTP, `H5DB_SFTP_PROFILE` reports backend
reads, successful libssh2 read returns, and returned payload bytes. These SFTP counters exclude protocol overhead
and do not measure all speculative read-ahead traffic.

Both backends report fetched payload bytes to DuckDB's query profiler, including reads with caching disabled.
Cache hits do not add fetched bytes; transport overhead and speculative SFTP read-ahead are not included.
