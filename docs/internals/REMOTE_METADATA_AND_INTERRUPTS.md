# Remote Metadata Walks And Interruptibility

> Status: The remote metadata analysis in this document still applies, but some
> implementation details have moved on:
> - `h5_tree` now uses explicit resumable `H5Literate_by_name2` traversal and
>   returns rows directly from the DuckDB execution thread
> - `h5_ls` now exists as the shallow-listing API discussed below
> - table `h5_tree` and `h5_ls` now make HDF5 metadata and attribute reads
>   projection-aware
> - the interruptibility discussion remains a design note; transport details in
>   DuckDB-backed remote filesystems may change across DuckDB versions

## Scope

This note captures two related conclusions from investigating slow remote HDF5 queries:

1. Why remote `h5_tree(...)` can be extremely slow on some files.
2. What a reasonable, maintainable solution looks like for query interruptibility.

The conclusions here are grounded in:

- `h5_tree` implementation in [src/h5_tree.cpp](../../src/h5_tree.cpp)
- current remote VFD implementation in [src/h5_remote_vfd.cpp](../../src/h5_remote_vfd.cpp)
- DuckDB's remote filesystem stack
- direct HTTP tracing against a public S3-backed HDF5 object

## Investigated File

The concrete slow case investigated was:

```sql
EXPLAIN ANALYZE
FROM h5_tree('s3://terrafusiondatasampler/P233/TERRA_BF_L1B_O12236_20020406135439_F000_V001.h5');
```

The backing object is:

- `https://terrafusiondatasampler.s3.amazonaws.com/P233/TERRA_BF_L1B_O12236_20020406135439_F000_V001.h5`
- `Content-Length = 54,614,253,313` bytes
- approximately `50.9 GiB`

## Why `h5_tree` Is Slow

### What `h5_tree` does today

`h5_tree` performs a recursive namespace walk. It traverses one group at a time
with resumable `H5Literate_by_name2` calls and returns rows in DuckDB-sized
batches. Each iteration slice holds the global HDF5 mutex only while it is using
HDF5; the mutex is released before the batch is returned to DuckDB.

Every recursive scan must:

- open the file
- traverse the namespace from `/`
- resolve local links to determine their object type and identity
- identify groups, avoid cycles, and descend into the groups it finds

Additional work depends on DuckDB's projected scan columns:

- `dtype` opens datasets and reads their datatypes only when required
- `shape` opens datasets and reads their dataspaces only when required
- each `h5_attr(...)` argument is read only when its output column is required
- columns used only by residual filters are still required and are therefore
  read
- a count-only scan uses a constant-`NULL` empty virtual column to carry row
  cardinality instead of forcing any real HDF5-backed output column

This means:

- recursive traversal can still be dominated by remote object-resolution
  latency
- object resolution during recursive traversal remains part of `h5_tree`
- unrequested dataset metadata and projected attributes add no HDF5 work
- resumable batches allow `LIMIT` and other early-stopping plans to avoid walking
  the entire namespace
- batching improves responsiveness, but it does not make first-run remote
  metadata exploration cheap on large scattered files

Table `h5_ls` has a cheaper path because it does not recurse. It always opens
and validates the requested group, but a path-only or count-only scan can emit
its links without resolving each child object. Selecting `type`, `dtype`,
`shape`, or projected attributes performs the resolution and metadata work
needed by those columns. Scalar `h5_ls` constructs its complete map and remains
eager.

### What was observed on the real file

A bounded 20-second probe made before metadata reads became projection-aware
used this query:

```sql
SELECT COUNT(*) FROM h5_tree('s3://...');
```

with HTTP logging enabled showed:

- `1 HEAD`
- `14 GET`
- about `13.85 MiB` total `GET` body bytes
- average `GET` latency about `1334 ms`
- min `GET` latency `380 ms`
- max `GET` latency `1916 ms`

The first `GET` ranges were at widely scattered offsets:

- `0`
- `12.88 GiB`
- `21.03 GiB`
- `32.18 GiB`
- `51.77 GiB`
- `52.27 GiB`
- `54.31 GiB`
- `54.61 GiB`

These request counts are a historical baseline, not the expected request count
for the current `COUNT(*)` query. The current implementation skips dataset
`dtype`, `shape`, and attribute reads for that query. The observation remains
useful because it shows how widely the file's namespace metadata is scattered.

This is the important fact:

- the query is latency-bound by scattered metadata lookups across a huge file
- it is not bandwidth-bound on dataset payload

In other words, the dominant problem is not "too much data transferred". It is "too many expensive random metadata round trips".

### What this means

For files like this, recursive traversal can remain expensive on remote storage
even after unrequested fields are removed:

- large file
- scattered metadata
- recursive object resolution
- synchronous execution

Selecting `dtype`, `shape`, or projected attributes adds the corresponding
per-object inspection. A full-row `FROM h5_tree(...)` query selects all of those
fields and therefore performs the full metadata work. A query such as
`SELECT path FROM h5_tree(...)` avoids that additional work, but it still has to
resolve objects to discover the recursive structure.

## Reasonable Solutions For Slow Remote `h5_tree`

### Conclusion

The implemented solution is to make metadata collection follow ordinary SQL
projection and to provide `h5_ls` for shallow exploration. Users can request a
full metadata result when needed without paying for those fields in a narrower
query.

### Recommended usage

Project only the columns needed by the query:

```sql
SELECT path, type
FROM h5_tree('s3://bucket/file.h5');
```

This still walks the complete recursive namespace, but it does not open every
dataset for `dtype` or `shape`.

When recursive discovery is unnecessary, list one group and project only its
paths:

```sql
SELECT path
FROM h5_ls('s3://bucket/file.h5', '/entry');
```

This can avoid per-child resolution entirely. `LIMIT` can stop either table
function at a batch boundary when the surrounding plan permits early stopping.
An `ORDER BY` generally requires consuming the complete input before returning
ordered rows.

Path and type filters are currently residual SQL filters; they do not prune the
recursive `h5_tree` walk. If recursive object resolution itself remains too
expensive, possible future APIs include a maximum depth, an explicit subtree
root, or traversal restricted to selected object kinds.

### What is likely not enough

The following may help repeated queries, but they do not solve the first-run problem for files like this:

- external file cache
- metadata block cache tuning
- minor VFD-level request coalescing improvements
- planner changes

Those are worthwhile, but they do not eliminate the remote metadata lookups
required by recursive object resolution.

### Recommendation

For remote usability, choose among these execution shapes deliberately:

1. `h5_tree`
   - recursive and cycle-safe
   - always resolves namespace objects, but reads only projected metadata and
     attributes

2. table `h5_ls`
   - shallow listing
   - can avoid child resolution in path-only and count-only queries

3. scalar `h5_ls`
   - returns one complete map value
   - eagerly reads all standard fields and declared projected attributes

This uses normal SQL projection rather than function-specific flags to control
metadata cost.

## Interruptibility

## What is happening today

DuckDB interrupt handling is cooperative:

- `ClientContext::Interrupt()` sets `interrupted = true`
- the executor notices it at normal execution boundaries

Relevant code:
- [duckdb/src/main/client_context.cpp](../../duckdb/src/main/client_context.cpp)
- [duckdb/src/parallel/pipeline_executor.cpp](../../duckdb/src/parallel/pipeline_executor.cpp)

The remote `h5db` path then blocks inside synchronous HDF5 calls:

- file open in [src/include/h5_raii.hpp](../../src/include/h5_raii.hpp)
- dataset reads in [src/h5_read.cpp](../../src/h5_read.cpp)

For DuckDB-backed remote files those HDF5 calls enter the VFD, then DuckDB's remote filesystem stack. In-flight
transport operations are generally synchronous from h5db's point of view, so h5db cannot reliably make them observe the
query interrupt flag until control returns from the filesystem call.

There is a second issue:

- `httpfs` treats request errors as retryable by default
- so even an aborted request can consume retry budget unless distinguished from ordinary network failure

## Recommended interruptibility solution

### Main recommendation

The transport-layer cancellation fix belongs in the underlying DuckDB remote filesystem, not in h5db.

Use whatever cancellation/progress mechanism that filesystem exposes and have it observe the current query interrupt
flag while the request is in flight.

### Why this is the right layer

This is the only clean way to make a thread blocked in remote I/O become interruptible without:

- killing threads
- closing HDF5 handles from another thread
- relying on tiny timeouts
- weakening correctness

DuckDB's remote filesystems are initialized while a `ClientContext` is available. That is the right layer to carry
query-scoped cancellation state into transport requests.

### Important requirement

A user interrupt must be non-retryable.

If a transport request aborts because the query was cancelled, the remote filesystem must not treat that as an ordinary
retryable request failure.

Practical options:

1. throw `InterruptException` directly
2. add a non-retryable request-error marker
3. add an explicit "cancelled" response/error kind

The key property is:

- user interrupt must bypass the normal HTTP retry path

## Recommended `h5db` follow-up changes

Table `h5_tree` already checks for interruption before scans, between traversal
slices, and inside its link callback. Table `h5_ls` checks before each batch.
Further work should focus on operations that cannot currently observe an
interrupt while they are blocked:

1. Consider making `WaitForFetchComplete(...)` interruptible only if transport-layer cancellation lands first. The
   current repository intentionally leaves this wait blocking.
2. When a remote HDF5 call fails because the underlying request was cancelled,
   translate that to `InterruptException`, not `IOException`.

These are smaller changes, but they improve responsiveness and make cancellation semantics cleaner.

## What should not be done

The following approaches are not recommended:

- asynchronous closing of HDF5 handles from another thread
- thread killing
- trying to force cancellation from inside HDF5 internals
- relying on short HTTP timeouts as the main cancellation mechanism

These are brittle and are likely to create correctness or maintenance problems.

## Summary

### Slow remote `h5_tree`

Reasonable solution:

- project only the metadata columns the query needs
- use table `h5_ls` for shallow exploration; path-only and count-only scans can
  avoid resolving children
- recursive `h5_tree` still has to resolve objects and can therefore remain
  expensive on huge files with scattered metadata

### Interruptibility

Reasonable solution:

- yes
- implement in-flight cancellation in the underlying DuckDB remote filesystem
- mark user-cancelled requests as non-retryable
- retain cooperative checks between HDF5 operations in `h5db`

That combination is the cleanest path that is both maintainable and technically sound.
