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
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const maxLogFiles = 100

var (
	logger      *slog.Logger
	seenFiles   sync.Map
	projectRoot string
)

func init() {
	// Grab the current working directory when the program starts
	projectRoot, _ = os.Getwd()
}

func formatFilePath(fullPath string) string {
	val, alreadySeen := seenFiles.LoadOrStore(fullPath, "")

	if !alreadySeen {
		shortPath := ""

		if projectRoot != "" {
			rel, err := filepath.Rel(projectRoot, fullPath)
			if err == nil && !strings.HasPrefix(rel, "..") {
				shortPath = rel
			}
		}

		// Fallback: If Rel failed or produced an ugly path
		if shortPath == "" {
			if idx := strings.Index(fullPath, "cluster-director-mcp/"); idx != -1 {
				shortPath = fullPath[idx+len("cluster-director-mcp/"):]
			} else {
				shortPath = filepath.Base(fullPath)
			}
		}

		seenFiles.Store(fullPath, shortPath)
		return fullPath
	}

	shortPath := val.(string)
	if shortPath == "" {
		return fullPath
	}

	return shortPath
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

func WriteToLog(message string) {
	message = strings.ReplaceAll(message, "\n", " | ")

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
	timeStr := r.Time.Format(time.RFC3339)

	sourceStr := ""
	if h.opts.AddSource && r.PC != 0 {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		f, _ := fs.Next()
		displayPath := formatFilePath(f.File)
		sourceStr = fmt.Sprintf("%s:%d", displayPath, f.Line)
	}

	levelStr := r.Level.String()

	_, err := fmt.Fprintf(h.w, "%s %s %s %s\n", timeStr, levelStr, sourceStr, r.Message)
	return err
}

func (h *PlainHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *PlainHandler) WithGroup(name string) slog.Handler       { return h }
