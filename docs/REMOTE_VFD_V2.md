# Remote VFD V2

> Status: This is a design/implementation note, not a description of missing functionality. The current repository now
> opens remote files through `CachingFileSystem::OpenFile(QueryContext(...), ...)`, uses `FILE_FLAGS_DIRECT_IO`, keeps a
> VFD-managed small-read block cache, preserves remote error text, and has remote cache/failure tests. Treat the
> remaining text as rationale plus remaining ideas rather than as a statement of the current repo being pre-V2.

## Goal

Improve remote HDF5 read performance while preserving correctness and integrating with DuckDB's `httpfs` and external file cache.

## Key Findings

### 1. The pre-V2 implementation bypassed DuckDB's external file cache

The remote VFD originally opened remote files through `FileSystem::OpenFile`, not `CachingFileSystem::OpenFile`.

Consequence:
- repeated remote HDF5 scans re-fetch byte ranges
- `enable_external_file_cache` has no effect on `h5_read`
- bind/init duplicate metadata GETs because body bytes are never cached globally

### 2. `httpfs` already has two different cache layers

From `duckdb-httpfs`:
- metadata cache for `HEAD` results
- external file cache for body byte ranges

These solve different problems. The metadata cache only avoids repeated `HEAD`s. It does not cache response bodies.

### 3. HDF5 can issue very small VFD reads for chunked datasets

This was verified against local fixtures by temporarily forcing `DIRECT_IO` so that HTTP `Range` requests closely reflected VFD read sizes.

Observed examples:
- `test/data/nd_cache_test.h5:/array_2d_chunked_small`
  - dataset chunk shape: `(128, 6)`
  - chunk payload size: `128 * 6 * 4 = 3072` bytes
  - observed HTTP ranges included many consecutive `3072`-byte GETs
- `test/data/nd_cache_test.h5:/array_2d_chunked_partial`
  - dataset chunk shape: `(10, 5)`
  - chunk payload size: `10 * 5 * 4 = 200` bytes
  - observed HTTP ranges included many consecutive `200`-byte GETs
- `test/data/nd_cache_test.h5:/tensor_4d_chunked_small`
  - dataset chunk shape: `(256, 2, 2, 2)`
  - chunk payload size: `256 * 2 * 2 * 2 * 4 = 8192` bytes
  - observed many `8192`-byte GETs

Implication:
- a v2 that uses `DIRECT_IO` plus exact-range reads for all raw dataset traffic would be worse than the implementation
  this note started from on chunked datasets

### 4. Small raw reads are spatially clustered

For the chunked fixtures above, the tiny VFD reads were mostly ascending and clustered within a small number of nearby file regions.

Implication:
- block coalescing at the VFD layer is viable
- a modest block cache should collapse many tiny reads into a few larger network reads

### 5. Large non-chunked reads still exist

For large simple datasets, HDF5 issued large raw reads on the order of several MB.

Implication:
- the VFD should not force all reads through fixed-size blocks
- large reads should stay exact

## Design

### Summary

V2 should use:
- `CachingFileHandle` underneath the VFD
- `DIRECT_IO` on the underlying `httpfs` handle
- a VFD-managed block cache for all small reads, including `H5FD_MEM_DRAW`
- exact reads for large requests

This keeps the good part of that architecture, coalescing tiny HDF5 reads, while moving the fetched bytes into
DuckDB's shared external file cache.

### Open Path

Open remote files through:
- `CachingFileSystem::Get(context).OpenFile(QueryContext(context), OpenFileInfo(path), flags)`

Flags:
- `FILE_FLAGS_READ`
- `FILE_FLAGS_DIRECT_IO`

Reason:
- `DIRECT_IO` disables `httpfs`'s per-handle read buffer
- the VFD then fully controls coalescing behavior
- all fetched bytes are visible to DuckDB's shared external file cache

Do not use `FILE_FLAGS_NULL_IF_NOT_EXISTS` here.
- In practice it caused `httpfs` open failures such as HTTP 503 to collapse into a null-handle path
- the implementation relies on exceptions so those remote failures can be surfaced through HDF5

### Read Policy

Use two paths:

1. Small reads
- applies to metadata reads
- also applies to raw `H5FD_MEM_DRAW` reads smaller than a threshold
- service them through aligned fixed-size blocks

2. Large reads
- applies to raw reads at or above the threshold
- service them with exact `CachingFileHandle::Read`

### Block Cache

Maintain a per-open-file LRU of aligned blocks.

Recommended initial values:
- block size: `1MB`
- max tracked blocks per open file: `128`
- large-read threshold: `1MB`

Behavior:
- small reads are satisfied from one or more aligned `1MB` blocks
- block loads are performed through `CachingFileHandle::Read`
- returned `BlockHandle`s are retained in the VFD LRU for fast reuse within the open file
- because the underlying storage is the external file cache, those bytes are also reusable across queries

Why this is the right default:
- large enough to absorb tiny chunk reads
- matches the metadata block size used by this design
- still small enough to avoid pathological overfetch for scattered metadata

### Why not exact caching for all reads

Because HDF5 chunked datasets can drive the VFD with exact chunk payload reads that are far smaller than a sensible network transfer size.

Examples verified locally:
- `3072` bytes
- `200` bytes
- `8192` bytes

Exact caching for those reads would produce too many network round trips.

### Error Propagation

The VFD should preserve the underlying `httpfs` error text.

Plan:
- store the last remote error in thread-local state inside the VFD
- set it when open/read catches a DuckDB or standard exception
- consume it at the higher HDF5 call site when `H5Fopen` or `H5Dread` fails

This should turn generic messages like:
- `Failed to open HDF5 file`

into messages that retain HTTP details such as:
- status code
- auth failure
- timeout
- range request failure

## Implementation Plan

1. Replace the raw remote file handle with `CachingFileHandle`
2. Generalize the existing metadata cache into a small-read block cache
3. Route small `H5FD_MEM_DRAW` reads through the block cache
4. Route large `H5FD_MEM_DRAW` reads through exact `CachingFileHandle::Read`
5. Add thread-local remote error capture and higher-level error plumbing
6. Remove direct `BufferManager::Allocate` ownership for remote blocks
7. Add regression tests for:
   - external file cache population
   - repeated-query GET elimination
   - repeated-query HEAD elimination with `enable_http_metadata_cache`
   - improved remote error messages

## Acceptance Criteria

### Correctness

- existing remote tests still pass
- local and remote query results are unchanged
- remote HTTP failures surface concrete error details

### Performance

For repeated remote reads of the same file:
- the second query should show no repeated body GETs when the external file cache is enabled

For chunked small-dataset probes:
- the first query should perform block-sized GETs, not one network request per HDF5 chunk

For large contiguous datasets:
- large reads should remain coarse, not be broken into many `1MB` requests
