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

package cmd

import (
	"context"
	"fmt"
	"log"
	"os"

	"cluster-director-mcp/cluster-director-gke-ai/pkg/config"
	"cluster-director-mcp/cluster-director-gke-ai/pkg/install"
	"cluster-director-mcp/cluster-director-gke-ai/pkg/tools"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/spf13/cobra"
)

const (
	version = "0.0.1"
)

var (
	// rootCmd represents the base command when called without any subcommands
	rootCmd = &cobra.Command{
		Use:   "cluster-director-gke-ai",
		Short: "Cluster Director GKE AI Assistant",
		Run:   runRootCmd,
	}

	installCmd = &cobra.Command{
		Use:   "install",
		Short: "Install the Cluster Director GKE AI Assistant.",
	}

	installGeminiCLICmd = &cobra.Command{
		Use:   "gemini-cli",
		Short: "Install the Cluster Director GKE AI Assistant into your Gemini CLI settings.",
		Run:   runInstallGeminiCLICmd,
	}
)

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(installCmd)
	installCmd.AddCommand(installGeminiCLICmd)
}

func runRootCmd(cmd *cobra.Command, args []string) {
	startMCPServer(cmd.Context())
}

func startMCPServer(ctx context.Context) {
	c := config.New(version)
	impl := &mcp.Implementation{
		Name:    "Cluster Director GKE AI Assistant",
		Version: version,
	}
	s := mcp.NewServer(impl, &mcp.ServerOptions{})

	tools.Install(s, c)

	log.Printf("Starting Cluster Director GKE AI Assistant")
	tr := &mcp.LoggingTransport{Transport: &mcp.StdioTransport{}, Writer: log.Writer()}

	if err := s.Run(ctx, tr); err != nil {
		log.Printf("Server error: %v\n", err)
	}
}

func runInstallGeminiCLICmd(cmd *cobra.Command, args []string) {
	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current working directory: %v", err)
	}

	exePath, err := os.Executable()
	if err != nil {
		log.Fatalf("Failed to get executable path: %v", err)
	}

	if err := install.GeminiCLIExtension(wd, version, exePath); err != nil {
		log.Fatalf("Failed to install for gemini-cli: %v", err)
	}
	fmt.Println(os.Stderr, "Successfully installed Cluster Director GKE AI Assistant as a gemini-cli extension.")
}
