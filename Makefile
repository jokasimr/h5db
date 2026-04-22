PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=h5db
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

ifeq ($(OS),Windows_NT)
  H5DB_WINDOWS_TEST_ENV := 1
endif
ifneq ($(filter windows_%,$(DUCKDB_PLATFORM)),)
  H5DB_WINDOWS_TEST_ENV := 1
endif

H5DB_LOCAL_TEST_FILTERS := "test/sql/*" "~test/sql/remote/*"
ifeq ($(H5DB_WINDOWS_TEST_ENV),1)
  H5DB_LOCAL_TEST_FILTERS += "~test/sql/sftp_secret_validation.test"
endif

# Override test targets to ensure test data exists before running tests.
test_release_internal:
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	./build/release/$(TEST_PATH) $(H5DB_LOCAL_TEST_FILTERS)
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh --unittest-bin ./build/release/$(TEST_PATH)
ifneq ($(H5DB_WINDOWS_TEST_ENV),1)
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh --unittest-bin ./build/release/$(TEST_PATH)
else
	@echo "Skipping remote SFTP tests on Windows"
endif

test_debug_internal:
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	./build/debug/$(TEST_PATH) $(H5DB_LOCAL_TEST_FILTERS)
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh --unittest-bin ./build/debug/$(TEST_PATH)
ifneq ($(H5DB_WINDOWS_TEST_ENV),1)
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh --unittest-bin ./build/debug/$(TEST_PATH)
else
	@echo "Skipping remote SFTP tests on Windows"
endif

test_reldebug_internal:
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	./build/reldebug/$(TEST_PATH) $(H5DB_LOCAL_TEST_FILTERS)
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh --unittest-bin ./build/reldebug/$(TEST_PATH)
ifneq ($(H5DB_WINDOWS_TEST_ENV),1)
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh --unittest-bin ./build/reldebug/$(TEST_PATH)
else
	@echo "Skipping remote SFTP tests on Windows"
endif

test_remote_http:
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh

test_remote_sftp:
ifneq ($(H5DB_WINDOWS_TEST_ENV),1)
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh
else
	@echo "Skipping remote SFTP tests on Windows"
endif
