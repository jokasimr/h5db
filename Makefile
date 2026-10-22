PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=h5db
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
# DuckDB 2.0 requires an explicit version even when building a release tag.
OVERRIDE_GIT_DESCRIBE=v2.0.0

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

H5DB_LOCAL_TEST_FILTERS := "test/sql/*" "~test/sql/remote/*"

# Customize the CI tools' test hook for all build variants.
ifneq ($(SKIP_TESTS),1)
define RUN_TEST
	bash $(PROJ_DIR)test/data/ensure_test_data.sh
	./build/$(1)/$(TEST_PATH) $(H5DB_LOCAL_TEST_FILTERS)
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh --unittest-bin ./build/$(1)/$(TEST_PATH)
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh --unittest-bin ./build/$(1)/$(TEST_PATH)
endef
endif

test_remote_http:
	bash $(PROJ_DIR)test/scripts/run_remote_tests.sh

test_remote_sftp:
	bash $(PROJ_DIR)test/scripts/run_sftp_tests.sh
