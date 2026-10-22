PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=paimon
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
# DuckDB 2.0 no longer derives its version from git tags; keep in sync with the duckdb submodule.
OVERRIDE_GIT_DESCRIBE ?= v2.0.0
ENABLE_EXTENSION_AUTOLOADING ?= 1
ENABLE_EXTENSION_AUTOINSTALL ?= 1

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Exercise retained bindings alongside SQLLogicTests in local and CI entry points.
.PHONY: test_snapshot_binding_release test_snapshot_binding_debug test_snapshot_binding_reldebug
ifneq ($(SKIP_TESTS),1)
test_release: test_snapshot_binding_release
test_debug: test_snapshot_binding_debug
test_reldebug: test_snapshot_binding_reldebug
endif

test_snapshot_binding_release:
	cmake --build build/release --config Release --target paimon_check_snapshot_binding

test_snapshot_binding_debug:
	cmake --build build/debug --config Debug --target paimon_check_snapshot_binding

test_snapshot_binding_reldebug:
	cmake --build build/reldebug --config RelWithDebInfo --target paimon_check_snapshot_binding
