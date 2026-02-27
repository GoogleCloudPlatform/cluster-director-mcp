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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const LOCAL_HOST_SCRATCH_DIR = "cluster-director-mcp.scratch"

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

func ParseTime(dateStr string) (time.Time, bool) {

	layouts := []string{
		time.RFC3339,       // ISO 8601
		"2006-01-02",       // YYYY-MM-DD
		"01/02/2006",       // MM/DD/YYYY
		"02-01-2006 15:04", // DD-MM-YYYY HH:MM
		"Jan 2, 2006",      // Month Day, Year
		"02 Jan 2006",      // Date (DD Mon YYYY) e.g., "25 Oct 2023"
		"Jan 2",            // Date, Month (Mon DD) e.g., "Oct 25"
		"02",               // Just the day
	}

	parsedTime, formatUsed, err := parseWithFallback(dateStr, layouts)
	if err != nil {
		WriteToLog("Could not parse date string: " + dateStr)
		return time.Now(), false
	}

	// Post-processing: Infer missing data based on the format used
	now := time.Now()

	switch formatUsed {
	case "02":
		// Case: User gave only "Day". Use Current Year and Current Month.
		parsedTime = time.Date(now.Year(), now.Month(), parsedTime.Day(), 0, 0, 0, 0, time.Local)

	case "Jan 2":
		// Case: User gave "Month Day". Use Current Year.
		parsedTime = parsedTime.AddDate(now.Year(), 0, 0)
	}
	WriteToLog(fmt.Sprintf("Successfully parsed input date string %s \nParsed Time: %v\nFormat Used: %s\n", dateStr, parsedTime, formatUsed))

	return parsedTime, true
}

func parseWithFallback(input string, formats []string) (time.Time, string, error) {
	for _, layout := range formats {
		t, err := time.Parse(layout, input)
		if err == nil {
			return t, layout, nil
		}
	}
	return time.Time{}, "", errors.New("no matching time format found")
}

// SearchByColumn1 searches for a target string in the second column (index 1).
// It returns the found row and true, or nil and false if not found.
func SearchByColumn1(data [][]string, target string) ([]string, bool) {
	for _, row := range data {
		// SAFETY CHECK: Ensure the row has at least 2 columns (indices 0 and 1)
		// If we don't check this, a short row will cause a "panic: index out of range"
		if len(row) > 1 {

			// Option A: Exact Match (Case-Sensitive)
			if row[1] == target {
				return row, true
			}

			// Option B: Case-Insensitive Match (Uncomment to use)
			// if strings.EqualFold(row[1], target) {
			// 	return row, true
			// }
		}
	}
	return nil, false
}

// getLastLines scans the string and keeps a rolling slice of the last n lines.
func GetLastLines(s string, n int) string {
	var lines []string

	// Use a scanner to read the string line by line
	scanner := bufio.NewScanner(strings.NewReader(s))
	for scanner.Scan() {
		// Append the new line
		lines = append(lines, scanner.Text())

		// If we have more than n lines, drop the oldest one (at the front)
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	// We ignore scanner.Err() for this example

	// Join the remaining lines back together
	return strings.Join(lines, "\n")
}

func DeleteFile(filePathName string) bool {
	err := os.Remove(filePathName)
	if err != nil {
		WriteToLog(fmt.Sprintf("Failed to delete file: %s", filePathName))
		return false
	}
	return true
}

// dirExists checks if a directory exists at the given path.
func CheckFileOrDirExists(path string, checkIfItsDir bool) bool {
	// 1. Get FileInfo for the path.
	info, err := os.Stat(path)

	if err == nil {
		// 2. Path exists. Check if it's a directory.
		if checkIfItsDir {
			if info.IsDir() {
				WriteToLog(fmt.Sprintf("Directory exists: %s", path))
				return true
			}

			// Path exists but is a file, not a directory.
			WriteToLog(fmt.Sprintf("Path exists, but its not a directory: %s", path))
			return false
		}
		// Its a file and it exists
		WriteToLog(fmt.Sprintf("Path exists, its a file: %s", path))
		return true
	}

	// 3. Path does not exist.
	if os.IsNotExist(err) {
		WriteToLog(fmt.Sprintf("File or Directory does NOT exist: %s", path))
		return false
	}

	WriteToLog(fmt.Sprintf("Cannot determine if directory exists: %s", path))

	// 4. A different error occurred (e.g., permission issue).
	return false
}

func QueryURLAndGetResult(authToken string, url string) (string, bool) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		WriteToLog(fmt.Sprintf("Could create HTTP request object to to URL: %s", url))
		return "", false
	}

	req.Header.Set("Content-Type", "application/json")
	authHeader := fmt.Sprintf("Bearer %s", authToken)
	req.Header.Set("Authorization", authHeader)
	client := &http.Client{
		Timeout: 30 * time.Second, // Set a reasonable timeout.
	}

	resp, err := client.Do(req)
	if err != nil {
		WriteToLog("Could not making HTTP request to URL: " + url)
		return "", false
	}
	// Defer the closing of the response body.
	// This is important to free up network resources.
	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		WriteToLog("http.Get() did NOT return StatusOK")
		return "", false
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		WriteToLog("io.ReadAll(body) returned error. Returning ERROR")
		return "", false
	}

	bodyString := string(body)
	return bodyString, true
}

// containsAny checks if a string contains any of the substrings.
func StringMatchesAnySubstring(s string, substrings []string) bool {
	for _, sub := range substrings {
		if strings.Contains(s, sub) {
			return true // Found a match
		}
	}
	return false // No matches found
}

// contains checks if an integer is present in a slice.
func IntArrContains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func CreateScratchDir() bool {
	if CheckFileOrDirExists(LOCAL_HOST_SCRATCH_DIR, true) {
		return true
	}
	err := os.MkdirAll(LOCAL_HOST_SCRATCH_DIR, 0755)
	if err != nil {
		WriteToLog(fmt.Sprintf("Failed to create scratch directory: %s %v", LOCAL_HOST_SCRATCH_DIR, err))
		return false
	}
	return true
}

func SlurpFile(fileName string) (string, error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	return string(content), nil
}
