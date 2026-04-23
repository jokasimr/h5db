# Extension Updating

When updating h5db to a newer DuckDB release:

- Update submodules:
  - set `./duckdb` to the target tagged DuckDB release
  - set `./extension-ci-tools` to the matching release branch
- Update workflow pins in `./github/workflows`:
  - set the `duckdb_version` input in the `duckdb-stable-build` job in `MainDistributionPipeline.yml`
  - set the `duckdb_version` input in the `duckdb-stable-deploy` job in `MainDistributionPipeline.yml`
  - update the reusable workflow reference for `duckdb-stable-build` in
    `duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml`

# API Changes

h5db is built against DuckDB's internal C++ API, which is not guaranteed to be stable across DuckDB releases. After
updating the target DuckDB version, you may need follow-up code changes before the extension builds again.

DuckDB does not publish a dedicated changelog for internal C++ API changes, but the relevant changes are usually easy to
trace from source history.

Useful references:

- DuckDB [release notes](https://github.com/duckdb/duckdb/releases)
- DuckDB history of [core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- git history of the relevant DuckDB C++ headers
