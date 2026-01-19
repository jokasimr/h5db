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
./build/release/test/unittest "test/sql/*" "~test/sql/large/*"
```

## Run Everything

```bash
make test
```
