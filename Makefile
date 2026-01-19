PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=h5db
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Override test targets to ensure test data exists before running tests.
test_release_internal:
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	./build/release/$(TEST_PATH) "test/sql/*"

test_debug_internal:
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	./build/debug/$(TEST_PATH) "test/sql/*"

test_reldebug_internal:
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	./build/reldebug/$(TEST_PATH) "test/sql/*"
