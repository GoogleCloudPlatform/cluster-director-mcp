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
	"fmt"
	"os/exec"
	"strings"
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
	var b strings.Builder // Use a Builder to efficiently build the new string
	scanner := bufio.NewScanner(strings.NewReader(rawSSHOut))
	var ignoreLine bool
	for scanner.Scan() {
		line := scanner.Text()

		// Ignore empty lines
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

		// do no ignore this line
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
