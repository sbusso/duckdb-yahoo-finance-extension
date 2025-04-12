PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))


# Configuration of extension
EXT_NAME=yahoo_finance
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile