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

	WriteToLog("Executing 'gcloud auth print-access-token' to get bearer token...")

	// Prepare the command
	cmd := exec.Command("gcloud", "auth", "print-access-token")

	// Run the command and capture its output
	output, err := cmd.Output()
	if err != nil {
		// If 'gcloud' is not installed or not in the PATH, this will fail.
		// It can also fail if the user is not authenticated.
		WriteToLog(fmt.Sprintf("Error running gcloud command: %v", err))
		return false
	}

	// The output is a byte slice, so we convert it to a string and
	// trim any trailing newline or whitespace.
	authToken = strings.TrimSpace(string(output))
	WriteToLog("Successfully retrieved access token.")
	return true
}

// GetCachedAuthToken returns the currently cached OAuth token.
func GetCachedAuthToken() string {
	return authToken
}

func FilterString(rawSSHOut string, substringsToRemove []string) string {
	// Remove warning/useless strings from ssh output
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

	// Run the command and capture its output
	output, err := sshCmd.CombinedOutput()
	rawSSHOutput := strings.TrimSpace(string(output))
	filteredSSHOutput := FilterSSHOutput(rawSSHOutput)
	WriteToLog(string(filteredSSHOutput))
	if err != nil {
		// If 'gcloud' is not installed or not in the PATH, this will fail.
		// It can also fail if the user is not authenticated.
		WriteToLog(fmt.Sprintf("Error running SSH cmd: %s %v", cmd, err))
		return filteredSSHOutput, false
	}

	return filteredSSHOutput, true
}

func RunSCP(project string, zone string, srcFile string, destFile string) (string, bool) {
	// Prepare the command
	finalSCPCmd := exec.Command("/usr/bin/gcloud",
		"compute",
		"scp",
		"--project="+project,
		"--zone="+zone,
		"--tunnel-through-iap",
		srcFile,
		destFile)

	// Run the command and capture its output
	output, err := finalSCPCmd.CombinedOutput()
	scpOutput := strings.TrimSpace(string(output))
	filteredSCPOutput := FilterSSHOutput(scpOutput)
	WriteToLog(string(filteredSCPOutput))
	if err != nil {
		// If 'gcloud' is not installed or not in the PATH, this will fail.
		// It can also fail if the user is not authenticated.
		WriteToLog(fmt.Sprintf("Error running SCP: %v", err))
		return filteredSCPOutput, false
	}

	return filteredSCPOutput, true
}

// LogSearchType defines the type of log search to perform.
type LogSearchType int

const (
	LogSearchTypeUnknown LogSearchType = iota
	WereThereNCCLWarnMessages
	WereThereNCCLErrorMessages
	WereThereXidFailureMessagesInGkeCluster
	WereThereXidFailureMessagesInGkePod
	AreNCCLDebugLogsEnabled
	WereThereXidFailureMessagesInGceInstance
)

// SearchLogsCore executes a log search using the GCP SDK.
func SearchLogsCore(ctx context.Context, projectID string, filter string, maxResults int, searchType LogSearchType) (string, [][]string, bool) {
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

		payload := getPayloadString(entry)
		podName := entry.Resource.Labels["pod_name"]
		location := entry.Resource.Labels["location"]

		if searchType == WereThereXidFailureMessagesInGkeCluster {
			// We pass the raw payload back in the 3rd index.
			// GKE logic will parse the XID and Instance names locally.
			searchResults = append(searchResults, []string{podName, "", location, payload})
		} else {
			searchResults = append(searchResults, []string{payload})
		}

		countResults++
		if countResults >= maxResults {
			break
		}
	}

	return "Found Xid Errors", searchResults, true
}

// Helper to extract string content from either text or JSON payloads
func getPayloadString(entry *logging.Entry) string {
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
