# Testing h5db Extension

This directory contains all tests for the h5db extension. The `sql` directory holds tests written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html).

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

`make test` will generate any missing HDF5 test data before running tests. On POSIX platforms it also runs the
rewritten remote HTTP and rewritten remote SFTP suites, including the dedicated SFTP interaction harness. On Windows,
the SFTP harness and the SFTP-only SQL validation file are skipped.

## Large Tests

Large tests live in `test/sql/large/` as `*.test` and are included in `make test`. To skip them:
```bash
./build/release/test/unittest "test/sql/*" "~test/sql/large/*" "~test/sql/remote/*"
```

On Windows, add `~test/sql/sftp_secret_validation.test` to direct SQLLogicTest invocations.

## Run Everything

```bash
make test
```

## Run Suite Against Remote URLs

Run the full SQL suite against rewritten remote paths using the local range-capable HTTP endpoint:

```bash
make test_remote_http
```

## Run Suite Against SFTP URLs

Run the full SQL suite against rewritten `sftp://` paths using the local rooted SFTP server:

```bash
make test_remote_sftp
```

This target also runs the dedicated SFTP interaction harness in `test/scripts/run_sftp_interaction_tests.py`. It is
intended for POSIX platforms; on Windows it is skipped.

## Remote-Only SQLLogicTests

`test/sql/remote/*.test` contains remote HTTP-specific checks (auth, retries, redirects, timeout, truncation,
corruption, caching, and simulated server/drop errors).
These are included in `make test` through the remote runner and can also be executed via `make test_remote_http`.

## SFTP Interaction Harness

`test/scripts/run_sftp_tests.sh` rewrites the main SQL suite against `sftp://` URLs, starts the local SFTP test
server, and by default runs `test/scripts/run_sftp_interaction_tests.py` for auth, host-key verification, cache, and
disconnect cases. The runner will use the repo venv when present and otherwise falls back to `python3`/`python`,
installing `paramiko` if needed.

## TSAN Stress Harness

For race hunting beyond the normal SQLLogicTest coverage, use the dedicated TSAN stress runner:

```bash
GEN=ninja THREADSAN=1 make reldebug
./venv/bin/python test/scripts/tsan_stress.py --duckdb-binary ./build/reldebug/duckdb
```

The harness repeatedly exercises:
- projection/filter mismatches
- index and RSE pushdown intersections
- chunk-cache boundary cases
- sparse pushdown on cached columns
- logical partition ownership / batch-index plans
- large parallel `UNION ALL` scans
- randomized query interrupts

On this Linux host, TSAN binaries currently need ASLR disabled at launch time. The script applies `setarch x86_64 -R`
by default for that reason. Use `--allow-aslr` only if your environment does not need that workaround.
