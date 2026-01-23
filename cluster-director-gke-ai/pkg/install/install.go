// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package install

import (
	"cluster-director-mcp/genericCore"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func GeminiCLIExtension(baseDir, version, exePath string) error {
	extensionDir := filepath.Join(baseDir, ".gemini", "extensions", "cluster-director-mcp", "cluster-director-gke-ai")

	genericCore.WriteToLog(fmt.Sprintf("install.go:GeminiCLIExtension.0000.AAAA %s", baseDir))
	genericCore.WriteToLog(fmt.Sprintf("install.go:GeminiCLIExtension.0000.BBBB %s", exePath))
	genericCore.WriteToLog(fmt.Sprintf("install.go:GeminiCLIExtension.0000.CCCC %s", extensionDir))

	if err := os.MkdirAll(extensionDir, 0755); err != nil {
		genericCore.WriteToLog("install.go:GeminiCLIExtension.1111")
		return fmt.Errorf("could not create extension directory: %w", err)
	}

	genericCore.WriteToLog("install.go:GeminiCLIExtension.2222")

	// Create the manifest file as described in https://github.com/google-gemini/gemini-cli/blob/main/docs/extension.md.
	manifest := map[string]interface{}{
		"name":            "cluster-director-gke-ai",
		"version":         version,
		"description":     "Cluster Director GKE AI-Assistant to use, manage and monitor GKE Clusters",
		"contextFileName": baseDir + "/.gemini/extensions/cluster-director-mcp/GEMINI.md",
		"mcpServers": map[string]interface{}{
			"cluster-director-gke-ai": map[string]interface{}{
				"command": exePath,
			},
		},
	}

	genericCore.WriteToLog("install.go:GeminiCLIExtension.2222")

	manifestPath := filepath.Join(extensionDir, "gemini-extension.json")
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("could not marshal manifest.json: %w", err)
	}

	if err := os.WriteFile(manifestPath, data, 0644); err != nil {
		return fmt.Errorf("could not write manifest.json: %w", err)
	}

	// print to stderr
	fmt.Fprintf(os.Stderr, "Successfully installed Cluster Director GKE AI extension for Gemini CLI.\n")
	return nil
}
