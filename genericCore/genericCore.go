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
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const maxLogFiles = 100

var logger *slog.Logger

func WriteToLog(message string) {

	if logger == nil {
		f := CreateUniqueFilePath("logs/log.cluster-director-mcp")
		var writer io.Writer
		if f != nil {
			writer = f
		} else {
			writer = os.Stdout
		}

		opts := &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelInfo,
		}

		// Initialize the custom handler
		handler := &PlainHandler{
			w:    writer,
			opts: *opts,
		}

		logger = slog.New(handler)
		slog.SetDefault(logger)
	}

	// 1. Capture the Program Counter (PC) of the caller
	// We skip 2 frames:
	// 0 = runtime.Callers
	// 1 = WriteToLog
	// 2 = The function calling WriteToLog (e.g., clusterCore.go)
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])

	// 2. Create the record with the specific PC
	r := slog.NewRecord(time.Now(), slog.LevelInfo, message, pcs[0])

	// 3. Handle the record
	_ = logger.Handler().Handle(context.Background(), r)
}

type PlainHandler struct {
	w    io.Writer
	opts slog.HandlerOptions
}

// Enabled reports whether the handler handles records at the given level.
func (h *PlainHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.opts.Level.Level()
}

// Handle formats the record as a plain string without keys
func (h *PlainHandler) Handle(ctx context.Context, r slog.Record) error {
	// 1. Format Time
	timeStr := r.Time.Format(time.RFC3339)

	// 2. Format Source (File:Line)
	sourceStr := ""
	if h.opts.AddSource && r.PC != 0 {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		f, _ := fs.Next()
		sourceStr = fmt.Sprintf("%s:%d", f.File, f.Line)
	}

	// 3. Format Level
	levelStr := r.Level.String()

	// 4. Construct the final string: "TIME LEVEL SOURCE MESSAGE"
	_, err := fmt.Fprintf(h.w, "%s %s %s %s\n", timeStr, levelStr, sourceStr, r.Message)
	return err
}

func (h *PlainHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }

func (h *PlainHandler) WithGroup(name string) slog.Handler { return h }

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

func getUniqueLogFileName(logNameRoot string) string {
	for i := 0; i < maxLogFiles; i++ {
		_, err := os.Stat(fmt.Sprintf("%s.%d", logNameRoot, i))
		if err != nil && !os.IsNotExist(err) {
			return fmt.Sprintf("%s.%d", logNameRoot, i)
		}
	}

	return fmt.Sprintf("%s.%d", logNameRoot, 0)
}

func CreateUniqueFilePath(logNameRoot string) *os.File {
	// Make the directory if it does not exist, fail silently
	_ = os.MkdirAll(filepath.Dir(logNameRoot), 0755)
	logFile, err := os.OpenFile(getUniqueLogFileName(logNameRoot), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		// If we can't open the log file, it's a fatal error, so we exit.
		return nil
	}

	// logFile is intentionally not closed - its kept open
	return logFile
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
