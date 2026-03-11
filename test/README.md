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

`make test` will generate any missing HDF5 test data before running tests.

## Large Tests

Large tests live in `test/sql/large/` as `*.test` and are included in `make test`. To skip them:
```bash
./build/release/test/unittest "test/sql/*" "~test/sql/large/*" "~test/sql/remote/*"
```

## Run Everything

```bash
make test
```

## Run Suite Against Remote URLs

Run the full SQL suite against rewritten remote paths using the local range-capable HTTP endpoint:

```bash
make test_remote_http
```

On macOS, `make test` currently skips the remote HTTP portion of the suite; use `make test_remote_http` on Linux to
exercise the rewritten remote coverage.

## Remote-Only SQLLogicTests

`test/sql/remote/*.test` contains remote HTTP-specific checks (auth, retries, redirects, timeout, truncation,
corruption, caching, and simulated server/drop errors).
These are included in `make test` through the remote runner and can also be executed via `make test_remote_http`.
