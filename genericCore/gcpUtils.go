// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package genericCore

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"
)

var authToken string

// GetGCloudToken executes the 'gcloud auth print-access-token' command
// and caches the OAuth token.
func GetGCloudToken() bool {
	if authToken != "" {
		return true
	}

	WriteToLog("Fetching OAuth2 bearer token natively via ADC...")

	// Use the default context and cloud-platform scope
	ctx := context.Background()
	scopes := []string{"https://www.googleapis.com/auth/cloud-platform"}

	ts, err := google.DefaultTokenSource(ctx, scopes...)
	if err != nil {
		WriteToLog(fmt.Sprintf("Failed to find Default Token Source: %v", err))
		return false
	}

	token, err := ts.Token()
	if err != nil {
		WriteToLog(fmt.Sprintf("Error retrieving native token: %v", err))
		return false
	}

	authToken = token.AccessToken
	WriteToLog("Successfully retrieved access token natively.")
	return true
}

// GetCachedAuthToken returns the currently cached OAuth token.
func GetCachedAuthToken() string {
	return authToken
}

func FilterString(rawSSHOut string, substringsToRemove []string) string {
	var b strings.Builder
	scanner := bufio.NewScanner(strings.NewReader(rawSSHOut))
	var ignoreLine bool
	for scanner.Scan() {
		line := scanner.Text()

		if strings.TrimSpace(line) == "" {
			continue
		}

		ignoreLine = false
		for _, subString := range substringsToRemove {
			if strings.Contains(line, subString) {
				ignoreLine = true
				break
			}
		}
		if ignoreLine {
			continue
		}

		b.WriteString(line)
		b.WriteString("\n")
	}

	filteredResult := strings.TrimSuffix(b.String(), "\n")
	return filteredResult
}

func FilterSSHOutput(rawSSHOut string) string {
	return FilterString(rawSSHOut, []string{"Existing host keys found",
		"To increase the performance",
		"please see https:",
		"WARNING:"})
}

func RunSSHOnNode(hostName string, project string, zone string, cmd string) (string, bool) {
	sshCmd := exec.Command("/usr/bin/gcloud",
		"compute",
		"ssh",
		hostName,
		"--project="+project,
		"--zone="+zone,
		"--tunnel-through-iap",
		"--command",
		cmd)

	output, err := sshCmd.CombinedOutput()
	rawSSHOutput := strings.TrimSpace(string(output))
	filteredSSHOutput := FilterSSHOutput(rawSSHOutput)
	WriteToLog(string(filteredSSHOutput))
	if err != nil {
		WriteToLog(fmt.Sprintf("Error running SSH cmd: %s %v", cmd, err))
		return filteredSSHOutput, false
	}

	return filteredSSHOutput, true
}

func RunSCP(project string, zone string, srcFile string, destFile string) (string, bool) {
	finalSCPCmd := exec.Command("/usr/bin/gcloud",
		"compute",
		"scp",
		"--project="+project,
		"--zone="+zone,
		"--tunnel-through-iap",
		srcFile,
		destFile)

	output, err := finalSCPCmd.CombinedOutput()
	scpOutput := strings.TrimSpace(string(output))
	filteredSCPOutput := FilterSSHOutput(scpOutput)
	WriteToLog(string(filteredSCPOutput))
	if err != nil {
		WriteToLog(fmt.Sprintf("Error running SCP: %v", err))
		return filteredSCPOutput, false
	}

	return filteredSCPOutput, true
}

// LogProcessor is a callback function passed by the caller.
// It returns the formatted slice of strings to store, and a boolean indicating if it should be included.
type LogProcessor func(entry *logging.Entry) ([]string, bool)

// SearchLogsCore executes a log search using the GCP SDK.
// It delegates all parsing and filtering logic to the provided LogProcessor callback.
func SearchLogsCore(ctx context.Context, projectID string, filter string, maxResults int, processor LogProcessor) (string, [][]string, bool) {
	WriteToLog("-------------------SearchLogsCore()-------------------")

	client, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		WriteToLog("Could not create logging client")
		return fmt.Sprintf("Could not create logging client: %v", err), nil, false
	}
	defer client.Close()

	it := client.Entries(ctx, logadmin.Filter(filter))

	countResults := 0
	var searchResults [][]string

	for {
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Sprintf("Could not iterate over search results: %v", err), nil, false
		}

		// Let the callback function decide how to parse the entry, and if we should include it
		row, include := processor(entry)
		if include {
			searchResults = append(searchResults, row)
			countResults++
		}

		if countResults >= maxResults {
			break
		}
	}

	return "Search completed", searchResults, len(searchResults) > 0
}

// GetPayloadString extracts string content from either text or JSON payloads.
// Exported so the caller's callback function can use it.
func GetPayloadString(entry *logging.Entry) string {
	switch p := entry.Payload.(type) {
	case string:
		return p
	case *structpb.Struct:
		if val, ok := p.Fields["message"]; ok {
			return val.GetStringValue()
		}
		if val, ok := p.Fields["log"]; ok {
			return val.GetStringValue()
		}
		return p.String()
	default:
		return fmt.Sprintf("%v", p)
	}
}
