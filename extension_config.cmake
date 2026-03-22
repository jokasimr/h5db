# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(h5db
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Build httpfs locally for the remote test suite.
# We intentionally reuse DuckDB's own pinned httpfs config instead of hardcoding
# a commit here, so this repo stays aligned with whichever httpfs revision the
# vendored DuckDB checkout expects on the current branch.
include("${CMAKE_CURRENT_LIST_DIR}/duckdb/.github/config/extensions/httpfs.cmake")
