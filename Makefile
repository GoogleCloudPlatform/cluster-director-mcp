# Copyright 2025 Google Inc. All rights reserved.
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

GO := go

# Set your required minimum version here
REQUIRED_GO_VERSION := 1.24.8

# Extract the current version (e.g., turns "go version go1.22.3 ..." into "1.22.3")
CURRENT_GO_VERSION := $(shell go version 2>/dev/null | awk '{print $$3}' | sed 's/^go//')

all: build-cluster-director-slurm

build-cluster-director-slurm: check-go-version
	@echo ">> building cluster-director-slurm AI Assistant"
	@go build -o ./cluster-director-slurm/ ./cluster-director-slurm/...

clean:
	@rm -f *.test cluster-director-slurm/cluster-director-slurm
	@rm -rf _output/

check-go-version:
	@# Check if Go is installed at all
	@if [ -z "$(CURRENT_GO_VERSION)" ]; then \
		echo "Error: Go is not installed."; exit 1; \
	fi
	@# Use sort -V to compare semantic versions
	@if [ "$$(printf '%s\n' "$(REQUIRED_GO_VERSION)" "$(CURRENT_GO_VERSION)" | sort -V | head -n1)" != "$(REQUIRED_GO_VERSION)" ]; then \
		echo "Error: Go version $(REQUIRED_GO_VERSION) or newer is required (found $(CURRENT_GO_VERSION))."; \
		exit 1; \
	fi

.PHONY: all build clean check-go


