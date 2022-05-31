# Copyright 2022 The SODA Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# It's necessary to set this because some environments don't link sh -> bash.
SHELL := /usr/bin/env bash

# We don't need make's built-in rules.
MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

# Constants used throughout.
.EXPORT_ALL_VARIABLES:
OUT_DIR ?= _output
BIN_DIR := $(OUT_DIR)/bin
PRJ_SRC_PATH := github.com/soda-cdm/kahu
GENERATED_FILE_EXT := .pb.go

# Metadata for driving the build lives here.
META_DIR := .make

# This controls the verbosity of the build.  Higher numbers mean more output.
KUBE_VERBOSE ?= 1


define ALL_HELP_INFO
# Build code.
#
# Args:
#   WHAT: Directory names to build.  If any of these directories has a 'main'
#     package, the build will produce executable files under $(OUT_DIR)/bin.
#     If not specified, "everything" will be built.
#   GOFLAGS: Extra flags to pass to 'go' when building.
#   GOLDFLAGS: Extra linking flags passed to 'go' when building.
#   GOGCFLAGS: Additional go compile flags passed to 'go' when building.
#
# Example:
#   make
#   make all
#   make all WHAT=cmd/metaservice GOFLAGS=-v
#   make all GOLDFLAGS=""
#     Note: Specify GOLDFLAGS as an empty string for building unstripped binaries, which allows
#           you to use code debugging tools like delve. When GOLDFLAGS is unspecified, it defaults
#           to "-s -w" which strips debug information. Other flags that can be used for GOLDFLAGS
#           are documented at https://golang.org/cmd/link/
endef
.PHONY: all
ifeq ($(PRINT_HELP),y)
all:
	@echo "$$ALL_HELP_INFO"
else
all: verify
	build/build.sh $(WHAT)
endif

define CLEAN_HELP_INFO
# Remove all build artifacts.
#
# Example:
#   make clean
#
# TODO(thockin): call clean_generated when we stop committing generated code.
endef
.PHONY: clean
ifeq ($(PRINT_HELP),y)
clean:
	@echo "$$CLEAN_HELP_INFO"
else
clean:
	build/make-clean.sh
endif

define CLEAN_GENERATED_HELP_INFO
# Remove all auto-generated artifacts. Generated artifacts in staging folder should not be removed as they are not
# generated using generated_files.
#
# Example:
#   make clean_generated
endef
.PHONY: clean_generated
ifeq ($(PRINT_HELP),y)
clean_generated:
	@echo "$$CLEAN_GENERATED_HELP_INFO"
else
clean_generated:
	find . -type f -name '*$(GENERATED_FILE_EXT)'  | xargs rm -f
endif

define QUICK_RELEASE_IMAGES_HELP_INFO
# Build release images, but only for linux/amd64
#
# Args:
#   KUBE_FASTBUILD: Whether to cross-compile for other architectures. Set to 'false' to do so.
#   KUBE_BUILD_CONFORMANCE: Whether to build conformance testing image as well. Set to 'y' to do so.
#
# Example:
#   make quick-release-images
endef
.PHONY: quick-release-images
ifeq ($(PRINT_HELP),y)
quick-release-images:
	@echo "$$QUICK_RELEASE_IMAGES_HELP_INFO"
else
quick-release-images: KUBE_FASTBUILD = true
quick-release-images:
	build/release-images.sh
endif

define GENERATED_FILES_HELP_INFO
# Produce auto-generated files needed for the build.
#
# Example:
#   make generated_files
endef
.PHONY: generated_files
ifeq ($(PRINT_HELP),y)
generated_files:
	@echo "$$GENERATED_FILES_HELP_INFO"
else
generated_files: verify
	$(MAKE) -f Makefile.generate_proto $@ CALLED_FROM_MAIN_MAKEFILE=1
endif

define VERIFY_HELP_INFO
# Runs all the presubmission verifications.
#
# Example:
#   make verify
endef
.PHONY: verify
ifeq ($(PRINT_HELP),y)
verify:
	@echo "$$VERIFY_HELP_INFO"
else
verify:
	build/verify.sh
endif

