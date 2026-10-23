# Remote Metadata Walks and Interruptibility

This note describes metadata traversal and cancellation in the DuckDB 2.0 implementation. Transport and cache
details are documented in [Remote HDF5 Read Paths](REMOTE_VFD_V2.md).

## Metadata Traversal

`h5_tree` recursively traverses the namespace using resumable `H5Literate_by_name2` calls. It returns rows in
DuckDB-sized batches and releases `hdf5_global_mutex` between batches. Traversal resolves local links to identify
objects, discover groups, and avoid cycles.

Additional HDF5 work follows the projected columns:

- `dtype` reads dataset datatypes;
- `shape` reads dataset dataspaces;
- projected `h5_attr(...)` columns read the requested attributes;
- fields required by residual SQL filters are also read;
- a count-only scan uses an empty virtual column to carry cardinality without requesting real output fields.

Table `h5_ls` is shallow. It opens and validates the requested group, but path-only and count-only scans can emit
links without resolving each child object. Selecting other metadata or attributes adds the necessary child
resolution. Scalar `h5_ls` eagerly constructs its complete map.

`LIMIT` can stop table traversal at a batch boundary when the surrounding plan permits early stopping. An
`ORDER BY` generally requires consuming the complete input. Path and type predicates are residual filters; they do
not prune the recursive walk. Filename predicates can prune multi-file inputs before files are opened.

### Choosing a query

Project only the fields needed:

```sql
SELECT path, type FROM h5_tree('s3://bucket/file.h5');
```

For one group's children, avoid recursive discovery:

```sql
SELECT path FROM h5_ls('s3://bucket/file.h5', '/entry');
```

The recursive query still resolves objects to discover the group structure. Scattered metadata can therefore
remain latency-bound on a cold cache. The external file cache helps subsequent reads; it cannot eliminate the
first fetch of each required range.

### Historical observation

A 20-second probe of `COUNT(*) FROM h5_tree(...)`, made before projection-aware metadata reads, examined:

```text
s3://terrafusiondatasampler/P233/TERRA_BF_L1B_O12236_20020406135439_F000_V001.h5
```

The file then reported 54,614,253,313 bytes. The probe observed one HEAD and fourteen GETs, transferring about
13.85 MiB with average GET latency of 1,334 ms. Ranges were spread throughout the roughly 50.9 GiB file. This
illustrates the cost of scattered metadata; these are historical measurements, not expected counts or timings for
the current implementation.

## Interruptibility

DuckDB cancellation is cooperative. h5db checks `ClientContext::IsInterrupted()` before and between HDF5 operations;
`h5_tree` also checks inside traversal callbacks. The VFD checks around backend reads and preserves interruption
status when HDF5 callbacks return errors.

### DuckDB-backed transports

HDF5 calls enter the VFD and then the DuckDB filesystem. Cached reads can fetch multiple ranges asynchronously,
but the HDF5 call waits for them to finish. Checks in h5db cannot interrupt an individual blocked filesystem call;
in-flight cancellation depends on the underlying transport observing the query's interrupt state.

Transport cancellation should abort promptly and propagate an interruption without retrying it as an ordinary
network failure. h5db should preserve that distinction when translating HDF5 errors. Closing HDF5 handles from
another thread or forcing short HTTP timeouts does not provide a safe cancellation mechanism.

The table scanner's cache-window wait uses `someone_is_fetching.wait(...)`. It is notified when the fetching thread
finishes or fails; waking a waiter alone cannot cancel a blocked transport operation.

### Native SFTP

SFTP connection setup, authentication, directory operations, reads, and cleanup use nonblocking libssh2 operations
with interrupt checks between socket waits. Local DNS resolution and SSH-agent IPC have no additional h5db
cancellation mechanism. Cleanup becomes bounded when the connection is unreliable or the query is interrupted.

See [SFTP Interrupt and Cleanup Behavior](../developer/DEVELOPER.md#sftp-interrupt-and-cleanup-behavior) for the cleanup
deadline and [SFTP SSH-Agent Authentication](SFTP_SSH_AGENT_AUTH.md#runtime-behavior) for agent-specific details.
