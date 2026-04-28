PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=h5db
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

H5DB_LOCAL_TEST_FILTERS := "test/sql/*" "~test/sql/remote/*"

# Override test targets to ensure test data exists before running tests.
test_release_internal:
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	bash $(PROJ_DIR)test/scripts/run_windows_h5fopen_symlink_smoke.sh ./build/release/extension/h5db/h5fopen_symlink_smoke
	./build/release/$(TEST_PATH) $(H5DB_LOCAL_TEST_FILTERS)
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh --unittest-bin ./build/release/$(TEST_PATH)
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh --unittest-bin ./build/release/$(TEST_PATH)

test_debug_internal:
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	bash $(PROJ_DIR)test/scripts/run_windows_h5fopen_symlink_smoke.sh ./build/debug/extension/h5db/h5fopen_symlink_smoke
	./build/debug/$(TEST_PATH) $(H5DB_LOCAL_TEST_FILTERS)
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh --unittest-bin ./build/debug/$(TEST_PATH)
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh --unittest-bin ./build/debug/$(TEST_PATH)

test_reldebug_internal:
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	bash $(PROJ_DIR)test/scripts/run_windows_h5fopen_symlink_smoke.sh ./build/reldebug/extension/h5db/h5fopen_symlink_smoke
	./build/reldebug/$(TEST_PATH) $(H5DB_LOCAL_TEST_FILTERS)
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh --unittest-bin ./build/reldebug/$(TEST_PATH)
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh --unittest-bin ./build/reldebug/$(TEST_PATH)

test_remote_http:
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh

test_remote_sftp:
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh
