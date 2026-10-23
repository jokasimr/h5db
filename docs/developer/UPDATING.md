# Extension Updating

When updating h5db to a newer DuckDB release:

- Update submodules:
  - set `./duckdb` to the target tagged DuckDB release
  - set `./extension-ci-tools` to a compatible revision
- Set `OVERRIDE_GIT_DESCRIBE` in `Makefile` to the same DuckDB release tag. DuckDB 2.0 otherwise identifies a source
  build as a development version, even when the release tag is checked out.
- Update `.github/workflows/MainDistributionPipeline.yml`:
  - set `duckdb_version` in both `duckdb-stable-build` and `code-quality-check` to the release tag
  - pin both reusable workflow references and `ci_tools_version` inputs to the `extension-ci-tools` submodule commit
- Update the supported version in `README.md`.
- Run `make -j4`, `make test`, and `make format-check`. Verify that the shell reports the release version and that
  `h5db_version()` works without an explicit `LOAD h5db`.

The project targets one released DuckDB version. Commit both submodule pointers with the corresponding source,
workflow, and documentation changes.

`vcpkg.json` pins HDF5 to 1.14.6 while HDF5 2.x chunk-cache performance is evaluated separately. Its
`builtin-baseline` retains the previously tested dependency baseline; update dependency versions deliberately
when changing CI tools.

# API Changes

h5db is built against DuckDB's internal C++ API, which is not guaranteed to be stable across DuckDB releases. After
updating the target DuckDB version, you may need follow-up code changes before the extension builds again.

DuckDB does not publish a dedicated changelog for internal C++ API changes, but the relevant changes are usually easy to
trace from source history.

Useful references:

- DuckDB [release notes](https://github.com/duckdb/duckdb/releases)
- DuckDB history of [core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- git history of the relevant DuckDB C++ headers
