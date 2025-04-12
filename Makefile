PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))


# Configuration of extension
EXT_NAME=yahoo_finance
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
DUCKDB_VERSION=1.2.2
# We need this for testing
CORE_EXTENSIONS='httpfs'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Install extension to user's .duckdb directory
install:
	mkdir -p ~/.duckdb/extensions/v${DUCKDB_VERSION}/osx_arm64
	cp build/release/extension/${EXT_NAME}/${EXT_NAME}.duckdb_extension ~/.duckdb/extensions/v${DUCKDB_VERSION}/osx_arm64/
	@echo "Extension installed to ~/.duckdb/extensions/v${DUCKDB_VERSION}/"

