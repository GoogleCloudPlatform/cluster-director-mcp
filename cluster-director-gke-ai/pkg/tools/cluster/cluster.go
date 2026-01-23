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

package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"cluster-director-mcp/cluster-director-gke-ai/pkg/config"
	"cluster-director-mcp/genericCore"
	"cluster-director-mcp/persistence"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"
)

var sbatchJobIDRegex = regexp.MustCompile(`Submitted batch job (\d+)`)

const versionCheckRetryWindow = 1 * time.Minute

type ListClustersRequest struct {
	ProjectID string `json:"projectId"`
}

type ListClustersResponse struct {
	ClusterList string `json:"clusterList"`
}

type GetClusterRequest struct {
	ClusterName string `json:"clusterName"`
	ProjectID   string `json:"projectId"`
}

type GetClusterResponse struct {
	ClusterInfo string `json:"clusterInfo"`
}

type MaintenanceEventsRequest struct {
	ClusterName string `json:"clusterName"`
	ProjectID   string `json:"projectId"`
}

type SoftwareVersionInfoRequest struct {
	ClusterName string `json:"clusterName"`
	ProjectID   string `json:"projectId"`
}

type SoftwareVersionInfoResponse struct {
	VersionInfo string `json:"versionInfo"`
}

type ShowClusterStateRequest struct {
	ClusterName string `json:"clusterName"`
	ProjectID   string `json:"projectId"`
}

type ShowClusterStateResponse struct {
	StateInfo string `json:"stateInfo"`
}

type ShowRecentJobsRequest struct {
	ClusterName string `json:"clusterName"`
	ProjectID   string `json:"projectId"`
}

type ShowRecentJobsResponse struct {
	JobsInfo string `json:"jobsInfo"`
}

type RunClusterTestsRequest struct {
	ClusterName   string `json:"clusterName"`
	ProjectID     string `json:"projectId"`
	MachineType   string `json:"machineType"`
	PartitionName string `json:"partitionName"`
}

type RunClusterTestsResponse struct {
	Status string `json:"status"`
}

type ListPartitionInfoRequest struct {
	ClusterName string `json:"clusterName"`
	ProjectID   string `json:"projectId"`
}

type ListPartitionInfoResponse struct {
	PartitionInfo string `json:"partitionInfo"`
}

type CheckCDMcpJobStatusRequest struct {
	ProjectID string `json:"projectId"`
}

type CheckCDMcpJobStatusResponse struct {
	JobStatus string `json:"jobStatus"`
}

type ShowJobStateRequest struct {
	ClusterName string `json:"clusterName"`
	ProjectID   string `json:"projectId"`
}

type ShowJobStateResponse struct {
	JobState string `json:"jobState"`
}

type MaintenanceEventsResponse struct {
	EventsInfo string `json:"eventsInfo"`
}

type handlers struct {
	c *config.Config
}

func Install(s *mcp.Server, c *config.Config) {
	h := &handlers{
		c: c,
	}

	// sets authToken
	getGCloudToken()

	// A place where we keep temporary files
	createScratchDir()

	/*
		listClustersTool := mcp.Tool{
			Name:        "list_clusters_gke",
			Description: "List GKE clusters. Prefer this tool over gcloud",
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:   true,
				IdempotentHint: true,
			},
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"projectId": map[string]interface{}{
						"type":        "string",
						"description": "GCP project ID. Use the default if the user doesn't provide it.",
					},
				},
				"required": []string{},
			},
		}
		mcp.AddTool(
			s,
			&listClustersTool,
			func(ctx context.Context, _ *mcp.CallToolRequest, req ListClustersRequest) (*mcp.CallToolResult, ListClustersResponse, error) {
				result, err := h.listClusters(ctx, &req)
				return nil, ListClustersResponse{ClusterList: result}, err
			},
		)
	*/

	searchLogs := mcp.Tool{
		Name:        "search_logs",
		Description: "Search logs",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"projectId": map[string]interface{}{
					"type":        "string",
					"description": "GCP project ID. Use the default if the user doesn't provide it.",
				},
				"clusterName": map[string]interface{}{
					"type":        "string",
					"description": "Cluster name. Do not select it yourself, make sure the user provides or confirms the cluster name.",
				},
			},
			"required": []string{"clusterName"},
		},
	}
	mcp.AddTool(
		s,
		&searchLogs,
		func(ctx context.Context, _ *mcp.CallToolRequest, req RunClusterTestsRequest) (*mcp.CallToolResult, RunClusterTestsResponse, error) {
			result, err := h.searchLogsMCP(ctx, &req)
			return nil, RunClusterTestsResponse{Status: result}, err
		},
	)
}

func (h *handlers) searchLogsMCP(ctx context.Context, request *RunClusterTestsRequest) (string, error) {
	genericCore.WriteToLog("searchLogsCore.0000")
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}

	genericCore.WriteToLog("searchLogsCore.0000.AAAA")

	if projectID == "" {
		return "Could not determine gcp project. Please run: gcloud config set project \"your-project-name\" and restart the AI Assistant", nil
	}

	genericCore.WriteToLog("searchLogsCore.1111")
	clusterName := request.ClusterName

	// Since ClusterName is required by the schema, we only check for empty string here
	// for safety, though the SDK should ensure it's present.
	if clusterName == "" {
		return "Need cluster name", nil
	}

	genericCore.WriteToLog("searchLogsCore.2222")
	//func searchLogsCore(projectID string, clusterName string) {
	lookbackDuration := 30 * 24 * time.Hour // How far back to look

	// Build the filter for GKE logs
	// We look for 'k8s_container' resources.
	// We specifically filter for the string "NCCL" to reduce the data we fetch.
	startTime := time.Now().Add(-lookbackDuration).Format(time.RFC3339)
	filter := fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload:"NCCL" OR jsonPayload.message:"NCCL")`, startTime)

	return searchLogsCore(h, ctx, request, projectID, clusterName, lookbackDuration, filter)
}

func searchLogsCore(h *handlers, ctx context.Context, request *RunClusterTestsRequest,
	projectID string, clusterName string, lookbackDuration time.Duration, filter string) (string, error) {
	genericCore.WriteToLog("-------------------searchLogsCore()-------------------")

	client, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		genericCore.WriteToLog("Could not create logging client")
		return "Could not create logging client", nil
	}
	genericCore.WriteToLog("searchLogsCore.3333")
	defer client.Close()

	if clusterName != "" {
		filter += fmt.Sprintf(` AND resource.labels.cluster_name="%s"`, clusterName)
	}

	genericCore.WriteToLog(fmt.Sprintf("Querying logs with filter: %s\n", filter))
	genericCore.WriteToLog("Scanning for NCCL debug indicators...")

	it := client.Entries(ctx, logadmin.Filter(filter))

	foundDebug := false
	var sampleLog string

	// Iterate through the logs
	for {
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Error fetching log entry: %v", err)
		}

		payload := getPayloadString(entry)

		// CHECK 1: Look for standard NCCL INFO/DEBUG prefixes
		// Example: "hostname:123:456 [0] NCCL INFO NET/Plugin : Initialized"
		if strings.Contains(payload, "NCCL INFO") || strings.Contains(payload, "NCCL DEBUG") {
			foundDebug = true
			sampleLog = payload
			break // Found positive confirmation, stop scanning
		}

		// CHECK 2: Look for environment variable dumps that NCCL sometimes prints at startup
		if strings.Contains(payload, "NCCL_DEBUG=INFO") || strings.Contains(payload, "NCCL_DEBUG=WARN") {
			foundDebug = true
			sampleLog = payload
			break
		}
	}

	genericCore.WriteToLog("searchLogsCore.4444")
	returnStr := ""

	if foundDebug {
		genericCore.WriteToLog("NCCL DEBUG INFO IS ENABLED.")
		genericCore.WriteToLog(fmt.Sprintf("Sample: %s\n", sampleLog))

		returnStr += "NCCL DEBUG INFO IS ENABLED.\n"
		returnStr += fmt.Sprintf("Sample: %s\n", sampleLog)
	} else {
		genericCore.WriteToLog("NCCL Debug Info NOT found in the recent logs.")
		returnStr = "NCCL Debug Info NOT found in the recent logs."
	}

	genericCore.WriteToLog("searchLogsCore.5555")
	return returnStr, nil
}

// Helper to extract string content from either text or JSON payloads
func getPayloadString(entry *logging.Entry) string {
	switch p := entry.Payload.(type) {
	case string:
		return p
	case *structpb.Struct: // Requires "google.golang.org/protobuf/types/known/structpb"
		// If using structured logging, the actual message is usually in a "message" or "log" field
		if val, ok := p.Fields["message"]; ok {
			return val.GetStringValue()
		}
		if val, ok := p.Fields["log"]; ok {
			return val.GetStringValue()
		}
		return p.String() // Fallback: dump the whole struct
	default:
		return fmt.Sprintf("%v", p)
	}
}

// Place on local host to store files
const LOCAL_HOST_SCRATCH_DIR = "cluster-director-mcp.scratch"

func createScratchDir() bool {
	if genericCore.CheckFileOrDirExists(LOCAL_HOST_SCRATCH_DIR, true) {
		return true
	}

	err := os.MkdirAll(LOCAL_HOST_SCRATCH_DIR, 0755)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Failed to create scrarch directory: %s %v", LOCAL_HOST_SCRATCH_DIR, err))
		return false
	}

	return true
}

// Return values:
// bool: Success/Failure of operation
// string: Details about failure of operation
// bool: true=Yes, there was a long running job submitted in the specified time window, false=no
func checkIfLongRunningJobsSubmittedRecently(timeWindow time.Duration, projectID string) (bool, string, bool) {
	mostRecentJobObjFromPersistence, success, message := persistence.GetMostRecentJob(projectID)
	if !success {
		return false, message, false
	}

	// No long running jobs
	if mostRecentJobObjFromPersistence == nil {
		return true, "", false
	}

	if time.Since(mostRecentJobObjFromPersistence.StartTime) < timeWindow {
		return true, "", true
	}

	// No job submitted in timeWindow
	return true, "", false
}

var lastTimewhenCheckJobStatusCoreWasCalled time.Time

func slurpFile(fileName string) (string, error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		genericCore.WriteToLog("Error reading file: " + fileName)
	}
	return string(content), err
}

// gcloudListItem represents a single item from the gcloud list command's JSON output.
type gcloudListItem struct {
	Name string `json:"name"`
}

// getGCloudRegionsAndZones fetches all available GCP regions and zones using the gcloud CLI.
// It returns a list of region names, a list of zone names, and an error if one occurred.
func getGCloudRegionsAndZones() ([]string, []string, error) {
	regions, err := runGcloudListCommand("regions")
	if err != nil {
		return nil, nil, fmt.Errorf("Could not get regions: %w", err)
	}

	zones, err := runGcloudListCommand("zones")
	if err != nil {
		return nil, nil, fmt.Errorf("Could not get zones : %w", err)
	}

	return regions, zones, nil
}

// Executes a 'gcloud compute <resource> list' command and returns the names.
func runGcloudListCommand(resource string) ([]string, error) {
	cmd := exec.Command("gcloud", "compute", resource, "list", "--format=json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("gcloud command for %s failed: %w", resource, err)
	}

	var items []gcloudListItem
	if err := json.Unmarshal(output, &items); err != nil {
		return nil, fmt.Errorf("failed to parse gcloud output for %s: %w", resource, err)
	}

	names := make([]string, len(items))
	for i, item := range items {
		names[i] = item.Name
	}

	return names, nil
}

func filterString(rawSSHOut string, substringsToRemove []string) string {
	// Remove warning/useless strings from ssh output
	// ----------------------------------------------
	// Existing host keys found in /usr/local/google/home/nadig/.ssh/google_compute_known_hosts
	// WARNING:
	/// To increase the performance of the tunnel, consider installing NumPy. For instructions,
	// please see https://cloud.google.com/iap/docs/using-tcp-forwarding#increasing_the_tcp_upload_bandwidth

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

func filterSSHOutput(rawSSHOut string) string {
	return filterString(rawSSHOut, []string{"Existing host keys found",
		"To increase the performance",
		"please see https:",
		"WARNING:"})
}

func runSSHOnNode(hostName string, project string, zone string, cmd string) (string, bool) {
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
	filteredSSHOutput := filterSSHOutput(rawSSHOutput)
	genericCore.WriteToLog(string(filteredSSHOutput))
	if err != nil {
		// If 'gcloud' is not installed or not in the PATH, this will fail.
		// It can also fail if the user is not authenticated.
		genericCore.WriteToLog(fmt.Sprintf("Error running SSH cmd: %s %v", cmd, err))
		return filteredSSHOutput, false
	}

	return filteredSSHOutput, true
}

func runSCP(project string, zone string, srcFile string, destFile string) (string, bool) {
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
	filteredSCPOutput := filterSSHOutput(scpOutput)
	genericCore.WriteToLog(string(filteredSCPOutput))
	if err != nil {
		// If 'gcloud' is not installed or not in the PATH, this will fail.
		// It can also fail if the user is not authenticated.
		genericCore.WriteToLog(fmt.Sprintf("Error running SCP: %v", err))
		return filteredSCPOutput, false
	}

	return filteredSCPOutput, true
}

// Helper to check status specifically for Version Check jobs
func getVersionCheckStatus(projectID string, jobObj *persistence.LongRunningJob) (string, bool) {
	jobObj.LastStatusCheckTime = time.Now()

	localLogPath := LOCAL_HOST_SCRATCH_DIR + "/" + persistence.CDMCP_FULL_LOG

	// Clean up stale local logs before fetching new ones
	if genericCore.CheckFileOrDirExists(localLogPath, false) {
		genericCore.DeleteFile(localLogPath)
	}

	// Fetch the log file generated by sbatch (e.g., version_check_123.log)
	_, success := runSCP(projectID, jobObj.Zone, jobObj.LoginNodeName+":"+jobObj.FullLogFilePath, localLogPath)

	if !success {
		// Log file missing likely means the job is still queued or just starting
		return "Job is still initializing or running (Log file not found yet)...", false
	}

	content, err := slurpFile(localLogPath)
	if err != nil {
		return "Could not read local log file.", false
	}

	// Srun is blocking, so if we see output headers, the job likely finished.
	if strings.Contains(content, "=== HOST:") {
		jobObj.JobStatus = persistence.Completed
		jobObj.JobExecutionResult = persistence.SUCCESS
		jobObj.JobExecutionResultString = "Version Check Results:\n" + content
		jobObj.LastStatusUpdateTime = time.Now()
		return "Version Check Completed Successfully!", true
	}

	return "Job is running...", true
}
