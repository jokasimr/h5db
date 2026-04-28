# Remote Metadata Walks And Interruptibility

> Status: The remote metadata analysis in this document still applies, but some
> implementation details have moved on:
> - `h5_tree` now streams rows via a worker thread rather than doing all work in `Init`
> - `h5_ls` now exists as the shallow-listing API discussed below
> - the interruptibility discussion remains a design note; the current
>   repository intentionally keeps `WaitForFetchComplete(...)` as a blocking
>   wait in `src/h5_read.cpp`

## Scope

This note captures two related conclusions from investigating slow remote HDF5 queries:

1. Why remote `h5_tree(...)` can be extremely slow on some files.
2. What a reasonable, maintainable solution looks like for query interruptibility.

The conclusions here are grounded in:

- `h5_tree` implementation in [src/h5_tree.cpp](../../src/h5_tree.cpp)
- current remote VFD implementation in [src/h5_remote_vfd.cpp](../../src/h5_remote_vfd.cpp)
- DuckDB `httpfs` source in the public `duckdb-httpfs` repository
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

`h5_tree` still performs a full metadata walk over the namespace and still opens
datasets to fetch `dtype` and `shape`, even though rows are now streamed out by a
worker thread instead of being fully prepared in `Init`.

- open the file
- traverse the namespace from `/`
- for each dataset:
  - open the dataset
  - fetch type
  - fetch shape

This means:

- the query is still dominated by remote metadata latency
- dataset metadata amplification is still part of the `h5_tree` contract today
- streaming helps responsiveness, but it does not make first-run remote metadata
  exploration cheap on large scattered files

### What was observed on the real file

A bounded 20-second probe of the real DuckDB path:

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

This is the important fact:

- the query is latency-bound by scattered metadata lookups across a huge file
- it is not bandwidth-bound on dataset payload

In other words, the dominant problem is not "too much data transferred". It is "too many expensive random metadata round trips".

### What this means

For files like this, the current `h5_tree` contract is fundamentally expensive on remote storage:

- large file
- scattered metadata
- full object walk
- per-dataset metadata inspection
- synchronous execution

There is no realistic implementation trick inside the current semantics that will make first-run remote `h5_tree` cheap on these files.

## Reasonable Solutions For Slow Remote `h5_tree`

### Conclusion

Yes, there is a reasonable solution, but it is **not** "make the current full-fidelity `h5_tree` always fast remotely".

The reasonable solution is:

1. keep the current full-fidelity `h5_tree` semantics for users who explicitly want them
2. add a cheaper remote-oriented metadata mode or function with reduced semantics

### Recommended direction

Add a cheaper tree/listing API that avoids opening every dataset to fetch `dtype` and `shape`.

Examples of acceptable designs:

1. Add options to `h5_tree`
   - `include_dtype := false`
   - `include_shape := false`
   - `max_depth := N`
   - `shallow := true`

2. Add a separate function
   - `h5_ls(...)` now fills part of this role as a shallow immediate-child
     listing API
   - it still returns the full row shape (`path`, `type`, `dtype`, `shape`, and
     optional projected attributes), so it is cheaper mainly because it is
     shallow, not because it is metadata-light
   - a future lighter-weight mode could still choose to omit `dtype`/`shape`

3. Add selective traversal
   - filter to a subtree
   - filter to groups only
   - filter to datasets only

These are reasonable because they change the amount of required HDF5 metadata work, not just the implementation details.

### What is likely not enough

The following may help repeated queries, but they do not solve the first-run problem for files like this:

- external file cache
- metadata block cache tuning
- minor VFD-level request coalescing improvements
- planner changes

Those are worthwhile, but they do not change the fact that the query semantics require many remote metadata lookups.

### Recommendation

For remote usability, prefer a two-tier API:

1. `h5_tree`
   - full fidelity
   - potentially expensive remotely

2. `h5_ls`
   - shallow listing
   - intended for exploratory use over remote storage
   - still not a substitute for a future truly metadata-light listing mode if
     first-run remote latency becomes a bigger product concern

That is the cleanest way to make remote metadata exploration practical without weakening the current API contract unexpectedly.

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

For remote files those HDF5 calls enter the VFD, then `httpfs`, which currently uses synchronous `curl_easy_perform()` without a cancellation callback.

So an in-flight remote request does not observe the interrupt flag.

There is a second issue:

- `httpfs` treats request errors as retryable by default
- so even an aborted request can consume retry budget unless distinguished from ordinary network failure

## Recommended interruptibility solution

### Main recommendation

The transport-layer cancellation fix belongs in `httpfs`, not in `h5db`.

Use libcurl progress/cancel callbacks:

- `CURLOPT_NOPROGRESS = 0`
- `CURLOPT_XFERINFOFUNCTION`
- `CURLOPT_XFERINFODATA`

and have the callback observe the current query interrupt flag.

### Why this is the right layer

This is the only clean way to make a thread blocked in remote I/O become interruptible without:

- killing threads
- closing HDF5 handles from another thread
- relying on tiny timeouts
- weakening correctness

`httpfs` already has query-scoped state via `HTTPState`, and it is initialized while a `ClientContext` is available. That is the right place to carry a non-owning pointer to the current interrupt flag.

### Important requirement

A user interrupt must be non-retryable.

If curl aborts because the query was cancelled, `httpfs` must not treat that as an ordinary retryable request failure.

Practical options:

1. throw `InterruptException` directly
2. add a non-retryable request-error marker
3. add an explicit "cancelled" response/error kind

The key property is:

- user interrupt must bypass the normal HTTP retry path

## Recommended `h5db` follow-up changes

Even with the main fix in `httpfs`, `h5db` should still improve its own cooperative checks:

1. Check `context.interrupted` before entering expensive HDF5 operations.
2. Consider making `WaitForFetchComplete(...)` interruptible only if transport-layer cancellation lands first. The
   current repository intentionally leaves this wait blocking.
3. When a remote HDF5 call fails because the underlying request was cancelled, translate that to `InterruptException`, not `IOException`.

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

- yes, but only by adding a cheaper metadata-listing mode/function
- not by expecting the current full-fidelity remote traversal to become cheap on huge scattered files

### Interruptibility

Reasonable solution:

- yes
- implement in-flight HTTP cancellation in `httpfs`
- mark user-cancelled requests as non-retryable
- add a few cooperative interrupt checks in `h5db`

That combination is the cleanest path that is both maintainable and technically sound.
