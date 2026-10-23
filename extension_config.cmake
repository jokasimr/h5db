# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(h5db
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Keep the development shell and test runner preloaded with h5db.
duckdb_extension_statically_link(h5db)
