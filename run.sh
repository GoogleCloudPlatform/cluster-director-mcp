#!/bin/bash
# Copyright 2025 Google LLC
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

###############################################################################
# This script runs cluster-director-mcp
###############################################################################

export CLUSTER_DIRECTOR_MCP_DEBUG=1

# Update cluster-director-mcp if necessary
echo "----"
echo "Updating cluster-director-mcp..."
(git fetch --all 2>&1 > /dev/null ; git pull 2>&1 > /dev/null ; make -j  2>&1 > /dev/null) &
git_pull_make_pid=$!

# Clean scratch
echo "----"
echo "Cleaning Scratch space..."
mkdir -p cluster-director-mcp.scratch 2>&1 > /dev/null
rm -f cluster-director-mcp.scratch/* 2>&1 > /dev/null &

# Check if project is set
echo "----"
echo "Checking if project id is set ..."
PROJECT_ID=$(gcloud config get-value project)
if [[ -z "$PROJECT_ID" ]]; then
  echo "Error: Google Cloud project is not set. Please set a project using 'gcloud config set project YOUR_PROJECT_ID'."
  exit 1
else
  echo "Google Cloud project set to: $PROJECT_ID"
fi

# Check the user has permission to query IAM policy
echo "----"
echo "Checking if user $USER has permissions to get IAM policies for their project (permission to run: gcloud projects get-iam-policy "$PROJECT_ID") ..."
if gcloud projects get-iam-policy "$PROJECT_ID" \
  --flatten="bindings[].members" \
  --format='table(bindings.role, bindings.members)' \
  | grep -qE "does\s+not\s+have\s+permissions\s+" ; then
  echo "Failure: User does not have permissions to query IAM roles."
  echo "Please request the role roles/browser or roles/viewer from your project admin/owner of $PROJECT_ID"
  exit 1
else
  echo "Success: User has permissions to query IAM roles."
fi

# check IAM roles
if [[ "$1" != "--ignore_iam" ]]; then
  scripts/checkIAMRolesPresent.py
  exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    echo "Missing IAM roles, run with --ignore_iam to ignore (may result in some tools not working)"
  fi
fi

# Update gemini settings.json to install MCP servers
scripts/installExtensions.py ~/.gemini/settings.json

# Run cluster-director-mcp
echo "----"
echo -n "Running  based on gemini-cli version "
gemini --version
echo "..."
wait $git_pull_make_pid
if [ -n "$CDMCP_DEBUG" ]; then
    echo "CDMCP_DEBUG is defined."    
    gemini --debug --allowed-mcp-server-names  context7,cluster-director-mcp "$@"
else
    gemini --allowed-mcp-server-names  context7,cluster-director-mcp "$@"
fi
