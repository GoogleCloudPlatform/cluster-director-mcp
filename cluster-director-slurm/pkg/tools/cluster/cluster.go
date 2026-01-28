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
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cluster-director-mcp/cluster-director-slurm/pkg/config"
	"cluster-director-mcp/genericCore"
	"cluster-director-mcp/persistence"

	"github.com/modelcontextprotocol/go-sdk/mcp"
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

	// HCS does NOT support ALL regions and has an API to return the list of
	// regions it supports. Use HCS' API instead of GCE API to get ALL regions
	// because the GCE API is an overkill
	go getAllRegionsAndZonesSupportedByHCS(c.GetDefaultProjectID())

	// A place where we keep temporary files
	createScratchDir()
	// Text-output tool
	listClustersTool := mcp.Tool{
		Name:        "list_clusters",
		Description: "List clusters created using Cluster Director. Prefer this tool over gcloud",
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
		func(ctx context.Context, _ *mcp.CallToolRequest, req ListClustersRequest) (*mcp.CallToolResult, any, error) {
			result, err := h.listClusters(ctx, &req)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		},
	)
	// Structured-output tool
	getClusterTool := mcp.Tool{
		Name:        "get_cluster",
		Description: "Describe a cluster, i.e the type of compute nodes and storage provisioned. Prefer this tool over gcloud",
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
					"description": "The name of the Cluster. Do not select it yourself, make sure the user provides or confirms the cluster name.",
				},
			},
			"required": []string{"clusterName"},
		},
	}
	mcp.AddTool(
		s,
		&getClusterTool,
		func(ctx context.Context, _ *mcp.CallToolRequest, req GetClusterRequest) (*mcp.CallToolResult, GetClusterResponse, error) {
			result, err := h.getCluster(ctx, &req)
			return nil, GetClusterResponse{ClusterInfo: result}, err
		},
	)
	//Structured-output tool
	showClusterState := mcp.Tool{
		Name:        "show_cluster_state",
		Description: "Shows the state of the compute nodes in the cluster (idle, running jobs ..etc) created in Cluster Director. Prefer this tool over gcloud",
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
		&showClusterState,
		func(ctx context.Context, _ *mcp.CallToolRequest, req ShowClusterStateRequest) (*mcp.CallToolResult, ShowClusterStateResponse, error) {
			result, err := h.showClusterState(ctx, &req)
			return nil, ShowClusterStateResponse{StateInfo: result}, err
		},
	)
	// Text-output tool
	showJobState := mcp.Tool{
		Name:        "show_job_state",
		Description: "Shows the jobs running in cluster created using Cluster Director. Prefer this tool over gcloud",
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
		&showJobState,
		func(ctx context.Context, _ *mcp.CallToolRequest, req ShowJobStateRequest) (*mcp.CallToolResult, any, error) {
			result, err := h.showJobState(ctx, &req)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		},
	)
	// Text-output tool
	showRecentJobs := mcp.Tool{
		Name:        "show_recent_jobs",
		Description: "Shows the recent jobs that were run on the of cluster. Prefer this tool over gcloud",
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
		&showRecentJobs,
		func(ctx context.Context, _ *mcp.CallToolRequest, req ShowRecentJobsRequest) (*mcp.CallToolResult, any, error) {
			result, err := h.showRecentJobs(ctx, &req)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		},
	)
	// Text-output tool
	runNCCLTests := mcp.Tool{
		Name:        "run_nccl_test",
		Description: "Runs NCCL tests on the cluster's GPU nodes to verify cluster health. Prefer this tool over gcloud.",
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
				"machineType": map[string]interface{}{
					"type":        "string",
					"description": "Machine type (e.g., a3-megagpu-8g). Required if the cluster has multiple machine types.",
				},
				"partitionName": map[string]interface{}{
					"type":        "string",
					"description": "Partition name. Required if the cluster has multiple partitions.",
				},
			},
			"required": []string{"clusterName"},
		},
	}
	mcp.AddTool(
		s,
		&runNCCLTests,
		func(ctx context.Context, _ *mcp.CallToolRequest, req RunClusterTestsRequest) (*mcp.CallToolResult, any, error) {
			result, err := h.runNCCLTests(ctx, &req)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		},
	)
	// Text-output tool
	runDCGMTests := mcp.Tool{
		Name:        "run_dcgm_test",
		Description: "Runs DCGM tests on the cluster's GPU nodes to verify cluster health. Prefer this tool over gcloud",
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
				"machineType": map[string]interface{}{
					"type":        "string",
					"description": "Machine type (e.g., a3-megagpu-8g). Required if the cluster has multiple machine types.",
				},
				"partitionName": map[string]interface{}{
					"type":        "string",
					"description": "Partition name.",
				},
			},
			"required": []string{"clusterName"},
		},
	}
	mcp.AddTool(
		s,
		&runDCGMTests,
		func(ctx context.Context, _ *mcp.CallToolRequest, req RunClusterTestsRequest) (*mcp.CallToolResult, any, error) {
			result, err := h.runDCGMTests(ctx, &req)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		},
	)
	// Structured-output tool
	listPartitionInfo := mcp.Tool{
		Name:        "list_partition_info",
		Description: "Shows information on a slurm partition in a cluster created using Cluster Director. Prefer this tool over gcloud",
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
		&listPartitionInfo,
		func(ctx context.Context, _ *mcp.CallToolRequest, req ListPartitionInfoRequest) (*mcp.CallToolResult, ListPartitionInfoResponse, error) {
			result, err := h.listPartitionInfo(ctx, &req)
			return nil, ListPartitionInfoResponse{PartitionInfo: result}, err
		},
	)
	// Text-output tool
	checkCDMcpJobStatus := mcp.Tool{
		Name:        "check_job_status",
		Description: "Shows status of long running Job submitted by cluster-director-mcp in the last " + persistence.JOB_EXPIRY_TIME_WINDOW.String() + " hours. Prefer this tool over gcloud",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"projectId": map[string]interface{}{
					"type":        "string",
					"description": "GCP project ID. Use the default if not provided",
				},
			},
			"required": []string{},
		},
	}
	mcp.AddTool(
		s,
		&checkCDMcpJobStatus,
		func(ctx context.Context, _ *mcp.CallToolRequest, req CheckCDMcpJobStatusRequest) (*mcp.CallToolResult, any, error) {
			result, err := h.checkCDMcpJobStatus(ctx, &req)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		})
	// Text-output tool
	checkMaintenanceEvents := mcp.Tool{
		Name:        "check_maintenance",
		Description: "Checks for maintenance events for ALL the compute (GPU) nodes in the cluster. Prefer this tool over gcloud",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"projectId": map[string]interface{}{
					"type":        "string",
					"description": "GCP project ID. Use the default if not provided",
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
		&checkMaintenanceEvents,
		func(ctx context.Context, _ *mcp.CallToolRequest, req MaintenanceEventsRequest) (*mcp.CallToolResult, any, error) {
			result, err := h.checkMaintenanceEvents(ctx, &req)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		})
	// Structured-output tool
	showClusterSoftwareVersionInfo := mcp.Tool{
		Name:        "show_cluster_software_version_info",
		Description: "Show the software versions for ALL the compute (GPU) nodes in the cluster. Prefer this tool over gcloud",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"projectId": map[string]interface{}{
					"type":        "string",
					"description": "GCP project ID. Use the default if not provided",
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
		&showClusterSoftwareVersionInfo,
		func(ctx context.Context, _ *mcp.CallToolRequest, req SoftwareVersionInfoRequest) (*mcp.CallToolResult, SoftwareVersionInfoResponse, error) {
			result, err := h.showClusterSoftwareVersionInfo(ctx, &req)
			return nil, SoftwareVersionInfoResponse{VersionInfo: result}, err
		})

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

func (h *handlers) listClusters(ctx context.Context, request *ListClustersRequest) (string, error) {
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}
	genericCore.WriteToLog("-------------------listClusters()-------------------")
	genericCore.WriteToLog("projectId : " + projectID)
	clusterListString, _ := getClustersInAllRegions(projectID)
	return clusterListString, nil
}

func (h *handlers) getCluster(ctx context.Context, request *GetClusterRequest) (string, error) {
	clusterName := request.ClusterName
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}
	genericCore.WriteToLog("-------------------getCluster()-------------------")
	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("clusterName : " + clusterName)

	getClustersInAllRegions(h.c.GetDefaultProjectID())
	if clusterJSON, ok := clusterNames2JSON[clusterName]; ok {
		return clusterJSON, nil
	} else {
		return fmt.Sprintf("Could not get information on cluster %s in project %s", clusterName, projectID), nil
	}
}

func (h *handlers) checkMaintenanceEvents(ctx context.Context, request *MaintenanceEventsRequest) (string, error) {
	clusterName := request.ClusterName
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}

	genericCore.WriteToLog("-------------------checkMaintenanceEvents()-------------------")
	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("clusterName : " + clusterName)

	zone := getZoneForCluster(projectID, clusterName)
	if zone == "" {
		return fmt.Sprintf("Could not get zone for cluster %s in project %s", clusterName, projectID), nil
	}
	nodeList, success := getComputeNodesInCluster(clusterName+"-login-001", zone, projectID)
	if !success {
		return fmt.Sprintf("Could not get nodes in cluster %s in project %s", clusterName, projectID), nil
	}

	returnStr := ""
	for _, node := range nodeList {
		cmd := exec.Command("/usr/bin/gcloud", "compute", "instances", "describe", node, "--zone="+zone)
		output, err := cmd.Output()
		returnStr += "Maintenance info for node " + node + " : "
		if err != nil {
			returnStr += fmt.Sprintf("Could not get maintenance info for node %s : %w", node, err)
		} else if strings.Contains(string(output), "maintenanceStatus") {
			scanner := bufio.NewScanner(strings.NewReader(string(output)))
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "upcomingMaintenance:" {
					for i := 0; i < 5 && scanner.Scan(); i++ {
						returnStr += scanner.Text() + "\n"
					}
				}
			}
		} else {
			returnStr += " No events \n"
		}
	}
	return returnStr, nil
}

// Check node state before SSH'ing into the node
// TBD: Run checkAnyNodesNotInSafeToRunState and filter nodes that are in a safe state to SSH
// i.e not in idle, alloc..etc
// As shown below, some nodes in the same partition can be in different states
// ~$ sinfo
// PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
// part1*       up   infinite     59  down# xxxx-nodeset1-[0-35,37-39,41-45,47-50,52-59,61-63]
// part1*       up   infinite      5   idle xxxx-nodeset1-[36,40,46,51,60]

func (h *handlers) showClusterSoftwareVersionInfo(ctx context.Context, request *SoftwareVersionInfoRequest) (string, error) {
	genericCore.WriteToLog("-------------------showClusterSoftwareVersionInfo (Async Refactor)-------------------")

	// 1. Setup variables from Request Struct
	clusterName := request.ClusterName
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}

	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("clusterName : " + clusterName)

	// 2. Check for recent jobs (Rate Limiting)
	operationSuccessful, operationMesg, recentJob := checkIfLongRunningJobsSubmittedRecently(versionCheckRetryWindow, projectID)
	if !operationSuccessful {
		return operationMesg, nil
	}
	if recentJob {
		return fmt.Sprintf("A job was submitted recently. Please wait %s  before running version checks again.", versionCheckRetryWindow), nil
	}

	zone := getZoneForCluster(projectID, clusterName)
	if zone == "" {
		return fmt.Sprintf("Could not get zone for cluster %s in project %s", clusterName, projectID), nil
	}

	loginNode := clusterName + "-login-001"

	// 3. Verify Login Node Access
	sshOut, success := runSSHOnNode(loginNode, projectID, zone, "echo SUCCESS")
	if !success || !strings.Contains(sshOut, "SUCCESS") {
		return fmt.Sprintf("Could not SSH to login node %s. Is the cluster online?", loginNode), nil
	}

	// 4. Get Partition Information
	sinfoOutput, success := showClusterStateCore(projectID, zone, clusterName)
	if !success {
		return "Could not get cluster partition info via sinfo.", nil
	}

	partitions, _, success := parseOutputofSlurmSinfoCmdAndReturnPartitions(sinfoOutput)
	if !success || len(partitions) == 0 {
		return "Could not parse partitions from sinfo output.", nil
	}

	var partitionName string
	var targetNodes []string
	for k, v := range partitions {
		partitionName = k
		targetNodes = v
		break
	}

	nodeCount := len(targetNodes)
	if nodeCount == 0 {
		return "No compute nodes found in partition " + partitionName, nil
	}

	genericCore.WriteToLog(fmt.Sprintf("Targeting Partition: %s with %d nodes", partitionName, nodeCount))

	// 5. Create Persistence Job Object
	jobObj, _ := persistence.GetNewJob(clusterName, loginNode, zone, persistence.VERSION_CHECK, "N/A", partitionName, projectID)

	// 6. Generate the Slurm Script
	// Note: We use semicolons to prevent bash syntax errors when flattened
	cmdString := `
    echo "=== HOST: \$(hostname) ===";
    echo "--- OS Release ---";
    cat /etc/os-release | grep PRETTY_NAME;

    if command -v nvidia-smi &> /dev/null; then
        echo "--- GPU Info ---";
        nvidia-smi --query-gpu=driver_version,name --format=csv,noheader;
    else
        echo "--- GPU Info ---";
        echo "No NVIDIA GPU detected (CPU-only node)";
    fi;

    if command -v python3 &> /dev/null; then
        echo "--- PyTorch Version ---";
        python3 -c "import torch; print(torch.__version__)" 2>/dev/null || echo "PyTorch not installed";
    fi;
    echo "==========================";
    `

	flatCmd := strings.ReplaceAll(cmdString, "\n", " ")

	slurmScriptContent := []string{
		"#!/bin/bash",
		fmt.Sprintf("#SBATCH --job-name=version_check"),
		fmt.Sprintf("#SBATCH --output=version_check_%%j.log"),
		fmt.Sprintf("#SBATCH --partition=%s", partitionName),
		fmt.Sprintf("#SBATCH --nodes=%d", nodeCount),
		fmt.Sprintf("#SBATCH --ntasks-per-node=1"),
		"",
		fmt.Sprintf("srun --label /bin/bash -c '%s'", flatCmd),
	}

	localScriptName := LOCAL_HOST_SCRATCH_DIR + "/version_check.sbatch"
	file, err := os.Create(localScriptName)
	if err != nil {
		return "Failed to create local script: " + err.Error(), nil
	}
	for _, line := range slurmScriptContent {
		fmt.Fprintln(file, line)
	}
	file.Close()

	// 7. Deploy to Cluster
	runSSHOnNode(loginNode, projectID, zone, "rm -rf "+jobObj.RunDir+"; mkdir -p "+jobObj.RunDir)

	remoteScriptPath := jobObj.RunDir + "/version_check.sbatch"

	sshOut, success = runSCP(projectID, zone, localScriptName, loginNode+":"+remoteScriptPath)
	if !success {
		return "Failed to copy version check script to login node.", nil
	}

	// 8. Submit the Job
	submitCmd := fmt.Sprintf("cd %s && sbatch version_check.sbatch", jobObj.RunDir)
	sshOut, success = runSSHOnNode(loginNode, projectID, zone, submitCmd)
	if !success {
		return "Failed to submit sbatch job: " + sshOut, nil
	}

	// 9. Extract Job ID
	match := sbatchJobIDRegex.FindStringSubmatch(sshOut)
	if len(match) < 2 {
		return "Job submitted, but could not parse Job ID from output: " + sshOut, nil
	}
	slurmJobID, _ := strconv.Atoi(match[1])

	// 10. Persist Data
	jobObj.CDMcpJobId = slurmJobID
	jobObj.FullLogFilePath = fmt.Sprintf("%s/version_check_%d.log", jobObj.RunDir, slurmJobID)

	persistence.AppendNewJobDataAndWriteJobDataToDisk(jobObj)

	// Explicitly instruct the Model to STOP and not auto-invoke the status check.
	return fmt.Sprintf("Version check job submitted successfully (Slurm Job ID: %d). JOB_STARTED. STOP_HERE. Inform the user to wait 2 minutes, then run 'check_job_status' manually. DO NOT call check_job_status now.", slurmJobID), nil
}

func getComputeNodesInCluster(loginNode string, zone string, projectId string) ([]string, bool) {
	var returnArr []string
	sshOut, success := runSSHOnNode(loginNode, projectId, zone, "sinfo -N -l")
	if !success {
		return returnArr, success
	}

	scanner := bufio.NewScanner(strings.NewReader(sshOut))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) != 11 || fields[0] == "NODELIST" {
			continue
		}
		returnArr = append(returnArr, fields[0])
	}

	return returnArr, true
}

func (h *handlers) showClusterState(ctx context.Context, request *ShowClusterStateRequest) (string, error) {
	clusterName := request.ClusterName
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}
	if projectID == "" {
		return "Could not determine gcp project. Please run: gcloud config set project \"your-project-name\" and restart cluster-director-mcp", nil
	}
	zone := getZoneForCluster(projectID, clusterName)
	if zone == "" {
		return fmt.Sprintf("Could not get zone for cluster %s in project %s", clusterName, projectID), nil
	}

	sshOut, success := showClusterStateCore(projectID, zone, clusterName)
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not get cluster state!", nil
	}

	return sshOut, nil
}

func showClusterStateCore(projectId string, zone string, clusterName string) (string, bool) {
	sshOut, success := runSSHOnNode(clusterName+"-login-001", projectId, zone, "sinfo")
	return sshOut, success
}

func (h *handlers) showRecentJobs(ctx context.Context, request *ShowRecentJobsRequest) (string, error) {
	genericCore.WriteToLog("-------------------showRecentJobs()-------------------")
	clusterName := request.ClusterName
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}
	if projectID == "" {
		return "Could not determine gcp project. Please run: gcloud config set project \"your-project-name\" and restart cluster-director-mcp", nil
	}
	zone := getZoneForCluster(projectID, clusterName)
	if zone == "" {
		return fmt.Sprintf("Could not get zone for cluster %s in project %s", clusterName, projectID), nil
	}
	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("zone : " + zone)
	genericCore.WriteToLog("clusterName : " + clusterName)

	loginNode := clusterName + "-login-001"
	sshOut, success := runSSHOnNode(loginNode, projectID, zone, "sacct")
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not get recent jobs!", nil
	}
	return sshOut, nil
}

func checkAnyNodesNotInSafeToRunState(allClusterStates map[string]struct{}) bool {
	for key := range allClusterStates {
		keyLower := strings.ToLower(key)
		genericCore.WriteToLog("checking key : " + keyLower)
		// these are safe sates
		if strings.Contains(keyLower, "idle") ||
			strings.Contains(keyLower, "alloc") ||
			strings.Contains(keyLower, "mixed") {

			genericCore.WriteToLog("\t SAFE")
			continue
		}
		genericCore.WriteToLog("\t NOT SAFE")
		return false
	}

	genericCore.WriteToLog("\t FINAL SAFE")
	return true
}

// cluster.go (Refactored core handler method)

// [UPDATED] Signature changed to accept the generic RunClusterTestsRequest struct.
func runNCCLOrDCGMTestsCore(h *handlers, ctx context.Context, request *RunClusterTestsRequest, jobType persistence.LONG_RUNNING_OPERATION) (string, error) {
	genericCore.WriteToLog("-------------------runNCCLOrDCGMTestsCore()-------------------")

	// [UPDATED ARGUMENT LOGIC] Use struct fields instead of request.GetString/RequireString.
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}
	if projectID == "" {
		return "Could not determine gcp project. Please run: gcloud config set project \"your-project-name\" and restart cluster-director-mcp", nil
	}

	clusterName := request.ClusterName

	// Since ClusterName is required by the schema, we only check for empty string here
	// for safety, though the SDK should ensure it's present.
	if clusterName == "" {
		return "Need cluster name", nil
	}

	testName := ""
	if jobType == persistence.DCGM_TEST {
		testName = "DCGM"
	} else if jobType == persistence.NCCL_TEST {
		testName = "NCCL"
	} else {
		return "Currently only support running NCCL and DCGM tests", nil
	}

	twentyMins := time.Minute * 20
	operationSuccessful, operationMesg, thereWasARecentJob := checkIfLongRunningJobsSubmittedRecently(twentyMins, projectID)
	if !operationSuccessful {
		return operationMesg, nil
	}

	if thereWasARecentJob {
		return fmt.Sprintf("Please wait at least %s after a recent long running job submission", twentyMins), nil
	}

	zone := getZoneForCluster(projectID, clusterName)
	if zone == "" {
		return "Could not get zone for cluster " + clusterName + " in project " + projectID, nil
	}

	loginNode := clusterName + "-login-001"
	sshOut, success := runSSHOnNode(loginNode, projectID, zone, "echo SUCCESS")
	if !success || !strings.Contains(sshOut, "SUCCESS") {
		return "Could not SSH to login node " + loginNode + "  . Is the cluster still being created? Perhaps wait a few minutes until the login node has come online?", nil
	} else {
		genericCore.WriteToLog("Successfully able to SSH onto login node: " + loginNode)
	}

	// Get partitions and their states
	var sinfoOutput string
	sinfoOutput, success = showClusterStateCore(projectID, zone, clusterName)
	if !success {
		return genericCore.GetLastLines(sinfoOutput, 10) + "\nCould not run sinfo to get partitions in cluster " + clusterName + " in project " + projectID, nil
	} else {
		genericCore.WriteToLog("Successfully able to run sinfo on login node . Output of sinfo " + sinfoOutput)
	}

	partitions, clusterStates, success := parseOutputofSlurmSinfoCmdAndReturnPartitions(sinfoOutput)
	if !success {
		genericCore.WriteToLog("Error parsing output of sinfo: " + sinfoOutput)
		return sinfoOutput + "\nCould not parse output of sinfo", nil
	} else {
		genericCore.WriteToLog("Successfully able to parse output of sinfo ")
		for k, v := range partitions {
			genericCore.WriteToLog("partition: " + k + " , nodelist: " + strings.Join(v, " "))
		}
		for k := range clusterStates {
			genericCore.WriteToLog("cluster state: " + k)
		}
	}

	if !checkAnyNodesNotInSafeToRunState(clusterStates) {
		genericCore.WriteToLog("Some clusters did were not in idle/alloc/" + sinfoOutput)
		return "Your cluster is not ready to run jobs yet, maybe it is still being provisioned, or you have bad nodes, or you hit a stockout?", nil
	} else {
		genericCore.WriteToLog("All clusters are in safe state")
	}

	machineTypesInCluster := GetMachineTypeForCluster(projectID, clusterName)
	var machineType string
	if len(machineTypesInCluster) == 1 {
		machineType = machineTypesInCluster[0]
	} else {
		machineType = request.MachineType
		if machineType == "" {
			genericCore.WriteToLog("Machine Type is required but not provided.")
			return "Could not determine machine type for cluster " + clusterName + " in project " + projectID, nil
		}
	}
	if machineType != "a3-megagpu-8g" && machineType != "a3-ultragpu-8g" && machineType != "a4-highgpu-8g" {
		genericCore.WriteToLog("Machine Type  " + machineType)
		genericCore.WriteToLog("Machine type has to be one of a3-megagpu-8g, a3-ultragpu-8g, a4-highgpu-8g")
		return "Machine type has to be one of a3-megagpu-8g, a3-ultragpu-8g, a4-highgpu-8g", nil
	}

	var partitionName string
	if len(partitions) == 1 {
		for k := range partitions {
			partitionName = k
			break
		}
	} else {
		partitionName = request.PartitionName
		if partitionName == "" {
			genericCore.WriteToLog("Partition Name is required but not provided.")
			return "Could not determine Slurm Partition in cluster " + clusterName + " in project " + projectID, nil
		}
	}

	var nodeListName string
	if len(partitions[partitionName]) > 1 {
		nodeListName = strings.Join(partitions[partitionName], ",")
	} else if len(partitions[partitionName]) == 1 {
		nodeListName = partitions[partitionName][0]
	} else {
		return "Could not determine nodelist in Slurm Partition in cluster " + clusterName + " in project " + projectID + " for partition " + partitionName, nil
	}

	// Define the flags for opening the file.
	// os.O_CREATE: Create the file if it doesn't exist.
	// os.O_WRONLY: Open the file for writing only.
	// os.O_TRUNC: If the file already exists, truncate (empty) it.
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC

	// Define the file permissions.
	// 0755 is a common choice for executables:
	// - 7 (owner): read, write, execute
	// - 5 (group): read, execute
	// - 5 (others): read, execute
	permissions := os.FileMode(0755)

	// Create new long running job object, note persistence.NCCL_test must be overwritten with the correct value
	jobObj, _ := persistence.GetNewJob(clusterName,
		loginNode,
		zone,
		jobType,
		machineType, partitionName, projectID)

	// Create the file with the specified flags and permissions
	localScriptName := LOCAL_HOST_SCRATCH_DIR + "/" + persistence.CDMCP_SHELL_SCRIPT_NAME
	file, err := os.OpenFile(localScriptName, flags, permissions)
	if err != nil {
		return (err.Error() + "\nCould not create " + localScriptName + " file on local host"), nil
	}

	CDMcpJobIdString := fmt.Sprintf("# CDMcpJobId: %d\n", jobObj.CDMcpJobId)

	clusterDiagCmd := ""
	var summaryGenerationLines []string
	if jobType == persistence.NCCL_TEST {
		clusterDiagCmd = fmt.Sprintf(
			"python3 cli/cluster_diag.py -o slurm healthscan %s --check nccl --nodes %s --partition %s",
			machineType,
			nodeListName,
			partitionName)

		summaryGenerationLines = []string{"sed -n -e \"/HOST_VARS/,/NCCL version/p\" results/*.log >> ../cluster-director-mcp.summary.log",
			"sed -n -e \"/#[[:space:]]\\+size[[:space:]]\\+count/,/# Avg bus bandwidth/p\" results/*.log >> ../cluster-director-mcp.summary.log",
			"sed -n \"/Performing nccl check/,/NCCL test passing on all nodes/p\" ../../log.cluster-director-mcp_test >> ../cluster-director-mcp.summary.log"}
	} else {
		clusterDiagCmd = fmt.Sprintf(
			"python3 cli/cluster_diag.py -o slurm healthscan %s --check gpu --nodes %s --partition %s",
			machineType,
			nodeListName,
			partitionName)
		summaryGenerationLines = []string{
			"grep -m 3 -P \"(DCGM Version|Driver Version Detected|GPU Device IDs Detected)\" results/dcgmi*.out >> ../cluster-director-mcp.summary.log",
			"grep -P \".*(DCGM failed|DCGM diagnostics passing).*\" results/dcgmi*.out >> ../cluster-director-mcp.summary.log",
		}
	}

	// Write the script content to the file.
	linesToWrite := []string{
		"#!/bin/sh",
		"",
		CDMcpJobIdString,
		"rm -f cluster-director-mcp.summary.log",
		"git clone https://github.com/GoogleCloudPlatform/cluster-health-scanner.git",
		"pwd ; cd cluster-health-scanner ; pwd ; pip3 install -r cli/requirements.txt",
		"chmod +x ./deploy/slurm/cluster-validation.sh",
		clusterDiagCmd,
	}
	linesToWrite = append(linesToWrite, summaryGenerationLines...)

	for l := range linesToWrite {
		_, err = fmt.Fprintln(file, linesToWrite[l])
		if err != nil {
			file.Close()
			return err.Error() + "\nCould not write to " + localScriptName + " on local host", nil
		}
	}
	file.Close()

	// Create remote dir on login node
	sshOut, success = runSSHOnNode(loginNode, projectID, zone, "rm -rf "+jobObj.RunDir+"; mkdir -p "+jobObj.RunDir)
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not create dir " + jobObj.RunDir + " on login node " + loginNode + " in dir ", nil
	}
	sshOut, success = runSCP(projectID, zone, localScriptName, loginNode+":"+jobObj.RunScriptName)
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not copy " + persistence.CDMCP_SHELL_SCRIPT_NAME + " over to login node " + loginNode + " to location " + jobObj.RunScriptName, nil
	}

	sshOut, success = runSSHOnNode(loginNode, projectID, zone, "chmod +x "+jobObj.RunScriptName)
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not run \"chmod +x " + persistence.CDMCP_SHELL_SCRIPT_NAME + "\" on login node " + loginNode + " in dir " + jobObj.RunDir, nil
	}

	// Ignore return status
	_, _ = runSSHOnNode(loginNode, projectID, zone, "rm -f "+jobObj.RunDir+"/log.cluster-director-mcp_test")

	// Spawn a go routine - so we can run this asynchronously and return to the user
	go runSSHOnNode(loginNode, projectID, zone, "cd "+jobObj.RunDir+"; ./"+persistence.CDMCP_SHELL_SCRIPT_NAME+" 2>&1 > log.cluster-director-mcp_test")

	// Sleep 10 seconds, prevent check_job_status being called too soon
	time.Sleep(10 * time.Second)

	persistence.AppendNewJobDataAndWriteJobDataToDisk(jobObj)

	// Temporary comment end
	return testName + " tests running. Use check_job_status to get latest status on long running jobs", nil
}

func (h *handlers) runNCCLTests(ctx context.Context, request *RunClusterTestsRequest) (string, error) {
	return runNCCLOrDCGMTestsCore(h, ctx, request, persistence.NCCL_TEST)
}

// Returns a boolean to report probing job status - NOT status of job
// A value of true means, job status could be determined, false means
// it could not determine the status of the job s
func getNCCLOrDCGMTestsStatus(projectID string, ncclOrDCGMTestJobObj *persistence.LongRunningJob) (string, bool) {

	ncclOrDCGMTestJobObj.LastStatusCheckTime = time.Now()

	mainLogFileLocalPath := LOCAL_HOST_SCRATCH_DIR + "/" + persistence.CDMCP_FULL_LOG

	// Remove local copy of MAIN log
	if genericCore.CheckFileOrDirExists(mainLogFileLocalPath, false) && !genericCore.DeleteFile(mainLogFileLocalPath) {
		genericCore.WriteToLog(fmt.Sprintf("Could not delet local copy of main log file: %s", mainLogFileLocalPath))
		return "Could not delete logfile " + mainLogFileLocalPath + "  . Check job status later or verify status manually.", false
	}

	// Remove local copy of SUMMARY log
	summaryLogFileLocalFullPath := LOCAL_HOST_SCRATCH_DIR + "/" + persistence.CDMCP_SUMMARY_LOG
	if genericCore.CheckFileOrDirExists(summaryLogFileLocalFullPath, false) && !genericCore.DeleteFile(summaryLogFileLocalFullPath) {
		genericCore.WriteToLog(fmt.Sprintf("Could not delete local copy of summary log file: %s", summaryLogFileLocalFullPath))
		return "Could not delete logfile " + summaryLogFileLocalFullPath + "  . Check job status later or verify status manually.", false
	}

	// SCP over MAIN log
	sshOut, success := runSCP(projectID,
		ncclOrDCGMTestJobObj.Zone,
		ncclOrDCGMTestJobObj.LoginNodeName+":"+ncclOrDCGMTestJobObj.FullLogFilePath, mainLogFileLocalPath)
	if !success {
		persistence.WriteAllJobData()

		return string(genericCore.GetLastLines(sshOut, 10) + "\nNCCL/DCGM Test/Job is probably still running. I could not copy main logfile " + ncclOrDCGMTestJobObj.FullLogFilePath + " on cluster " + ncclOrDCGMTestJobObj.ClusterName + " over from login node " + ncclOrDCGMTestJobObj.LoginNodeName + "  . Check job status later or verify status manually."), false
	}

	// Read MAIN log
	mainLogContents, _ := slurpFile(mainLogFileLocalPath)

	// SCP over SUMMARY log
	sshOut, success = runSCP(projectID,
		ncclOrDCGMTestJobObj.Zone,
		ncclOrDCGMTestJobObj.LoginNodeName+":"+ncclOrDCGMTestJobObj.SummaryLogFilePath, summaryLogFileLocalFullPath)
	if success {
		ncclOrDCGMTestJobObj.JobExecutionResultString, _ = slurpFile(summaryLogFileLocalFullPath)
	} else {
		return string(genericCore.GetLastLines(sshOut, 10) + "\nNCCL Tests are probably still running. I could not copy the summary file " + persistence.CDMCP_SUMMARY_LOG + " on cluster " + ncclOrDCGMTestJobObj.ClusterName + " over from login node " + ncclOrDCGMTestJobObj.LoginNodeName + " . Check job status later or verify status manually."), false
	}

	status, result, summary := AnalyzeJobLog(ncclOrDCGMTestJobObj.JobType, mainLogContents)

	if status == persistence.Completed {
		ncclOrDCGMTestJobObj.JobStatus = status
		ncclOrDCGMTestJobObj.JobExecutionResult = result
		ncclOrDCGMTestJobObj.LastStatusUpdateTime = time.Now()
		ncclOrDCGMTestJobObj.JobExecutionResultString += summary
		return summary, true
	} else if ncclOrDCGMTestJobObj.JobType != persistence.NCCL_TEST && ncclOrDCGMTestJobObj.JobType != persistence.DCGM_TEST {
		genericCore.WriteToLog("Unsupported job type, only support NCCL or DCGM long running jobs")
		return "Unsupported job type, only support NCCL or DCGM long running jobs", false
	}

	// We could not determine if job has finished execution and result, check for strings
	// that indicate its running
	if genericCore.StringMatchesAnySubstring(mainLogContents,
		[]string{"Script Arguments",
			"Number of Nodes", "Basic check for required arguments passed"}) {

		ncclOrDCGMTestJobObj.JobStatus = persistence.Running
		ncclOrDCGMTestJobObj.JobExecutionResult = persistence.JOB_EXEC_RESULT_DONT_KNOW
		ncclOrDCGMTestJobObj.LastStatusUpdateTime = time.Now()
		return "", true

	} else {
		return "Could determine status of job ", false
	}
}

func (h *handlers) checkCDMcpJobStatus(ctx context.Context, request *CheckCDMcpJobStatusRequest) (string, error) {
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}

	if projectID == "" {
		return "Could not determine gcp project. Please run: gcloud config set project \"your-project-name\" and restart cluster-director-mcp", nil
	}
	return checkCDMcpJobStatusCore(projectID)
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

func verifyAndUpdateStatusOfRunningJobsOnClusterAndReturnListOfRunningJobs(projectId string) (string, bool) {
	mostRecentJobObjFromPersistence, success, message := persistence.GetMostRecentJob(projectId)
	if !success {
		return message, success
	}

	if mostRecentJobObjFromPersistence == nil {
		return "No running jobs found", true
	}

	returnMessage := ""
	returnSuccess := false
	jobStatusUpdated := false

	// Get ALL runnning cluster-director-mcp Jobs in cluster
	runningCDMcpJobInfoMap, success, message := GetDetailedJobInfoForAllRunningCDMcpJobsOfUserInCluster(
		mostRecentJobObjFromPersistence.ProjectId,
		mostRecentJobObjFromPersistence.ClusterName,
		mostRecentJobObjFromPersistence.Zone,
		mostRecentJobObjFromPersistence.LoginNodeName)

	_, persistenceJobFound := runningCDMcpJobInfoMap[mostRecentJobObjFromPersistence.CDMcpJobId]

	if !success {
		// Could not figure out anything about this job
		returnMessage += message + "\n"
	} else if persistenceJobFound {
		// We know for SURE that this job is running, mark it as such
		mostRecentJobObjFromPersistence.JobStatus = persistence.Running
		mostRecentJobObjFromPersistence.LastStatusCheckTime = time.Now()
		mostRecentJobObjFromPersistence.LastStatusUpdateTime = time.Now()

		returnMessage += "Job of type " + persistence.GetJobTypeString(int(mostRecentJobObjFromPersistence.JobType)) + " in cluster " + mostRecentJobObjFromPersistence.ClusterName + " is still running \n"
		jobStatusUpdated = true
	} else {
		// The job is NOT in the active running list (squeue).
		// It has likely finished, so we need to check the logs to see if it passed or failed.
		var m string
		var s bool

		// ROUTING LOGIC: Decide which helper to use based on the Job Type
		if mostRecentJobObjFromPersistence.JobType == persistence.VERSION_CHECK {
			// Use the new helper for Version Checks
			m, s = getVersionCheckStatus(mostRecentJobObjFromPersistence.ProjectId, mostRecentJobObjFromPersistence)
		} else {
			// Use the existing helper for NCCL/DCGM tests
			m, s = getNCCLOrDCGMTestsStatus(mostRecentJobObjFromPersistence.ProjectId, mostRecentJobObjFromPersistence)
		}

		returnMessage += mostRecentJobObjFromPersistence.JobExecutionResultString + m
		returnSuccess = s
		if s {
			jobStatusUpdated = true
		}
	}

	if jobStatusUpdated {
		persistence.WriteAllJobData()
	}

	return returnMessage, returnSuccess
}

func getCDMcpJobIdFromFile(cdMcpScript string) (int, bool) {
	fileH, err := os.Open(cdMcpScript)
	if err != nil {
		// If we can't open the log file, it's a fatal error, so we exit.
		return -1, false
	}
	defer fileH.Close()

	scanner := bufio.NewScanner(fileH)
	for scanner.Scan() {
		// Get the current line as a string
		line := scanner.Text()
		if strings.Contains(line, "CDMcpJobId:") {
			fields := strings.Fields(line)
			CDMcpJobId, err := strconv.Atoi(fields[1])
			if err != nil {
				genericCore.WriteToLog("getCDMcpJobIdFromFile Could not parse CDMcpJobId in line: " + line)
				return -1, false
			}
			return CDMcpJobId, true
		}
	}
	return -1, false
}

func checkCDMcpJobStatusCore(projectID string) (string, error) {
	genericCore.WriteToLog("-------------------checkCDMcpJobStatusCore() -------------------")

	twoMins := time.Minute * 2
	// Force 2-minute intervals between calls to check status
	if lastTimewhenCheckJobStatusCoreWasCalled.IsZero() {
		lastTimewhenCheckJobStatusCoreWasCalled = time.Now()
	} else if time.Since(lastTimewhenCheckJobStatusCoreWasCalled) < twoMins {
		return "Please wait at least 2 minutes before successive calls to check_job_status", nil
	}

	lastTimewhenCheckJobStatusCoreWasCalled = time.Now()
	operationSuccessful, operationMesg, thereWasARecentJob := checkIfLongRunningJobsSubmittedRecently(twoMins, projectID)
	if !operationSuccessful {
		return operationMesg, nil
	}

	if thereWasARecentJob {
		return "Please wait at least 2 minutes after job submission to check_job_status", nil
	}

	// This gets us status of running jobs
	mesg, _ := verifyAndUpdateStatusOfRunningJobsOnClusterAndReturnListOfRunningJobs(projectID)
	return mesg, nil
}

func slurpFile(fileName string) (string, error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		genericCore.WriteToLog("Error reading file: " + fileName)
	}
	return string(content), err
}

func (h *handlers) runDCGMTests(ctx context.Context, request *RunClusterTestsRequest) (string, error) {
	genericCore.WriteToLog("-------------------runDCGMTests()-------------------")
	return runNCCLOrDCGMTestsCore(h, ctx, request, persistence.DCGM_TEST)
}

func (h *handlers) showJobState(ctx context.Context, request *ShowJobStateRequest) (string, error) {
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}

	if projectID == "" {
		return "Could not determine gcp project. Please run: gcloud config set project \"your-project-name\" and restart cluster-director-mcp", nil
	}

	clusterName := request.ClusterName
	zone := getZoneForCluster(projectID, clusterName)
	if zone == "" {
		return "Could not get zone for cluster " + clusterName + " in project " + projectID, nil
	}

	genericCore.WriteToLog("-------------------showJobState()-------------------")
	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("zone : " + zone)
	genericCore.WriteToLog("clusterName : " + clusterName)

	loginNode := clusterName + "-login-001"
	sshOut, success := runSSHOnNode(loginNode, projectID, zone, "squeue")
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not run squeue to figure out job state on login node " + loginNode + " on cluster " + clusterName + " project " + projectID, nil
	}

	return sshOut, nil
}

func (h *handlers) listPartitionInfo(ctx context.Context, request *ListPartitionInfoRequest) (string, error) {
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}

	if projectID == "" {
		return "Could not determine gcp project. Please run: gcloud config set project \"your-project-name\" and restart cluster-director-mcp", nil
	}

	clusterName := request.ClusterName
	zone := getZoneForCluster(projectID, clusterName)
	if zone == "" {
		return fmt.Sprintf("Could not get zone for cluster %s in project %s", clusterName, projectID), nil
	}

	genericCore.WriteToLog("-------------------listPartitionInfo()-------------------")
	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("zone : " + zone)
	genericCore.WriteToLog("clusterName : " + clusterName)

	loginNode := clusterName + "-login-001"
	sshOut, success := runSSHOnNode(loginNode, projectID, zone, "scontrol show partition")
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not run scontrol on login node " + loginNode + " to figure out partition information", nil
	}

	return sshOut, nil
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
		return nil, nil, fmt.Errorf("failed to get regions: %w", err)
	}

	zones, err := runGcloudListCommand("zones")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get zones: %w", err)
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

	status, result, summary := AnalyzeJobLog(persistence.VERSION_CHECK, content)

	if status == persistence.Completed {
		jobObj.JobStatus = status
		jobObj.JobExecutionResult = result
		jobObj.JobExecutionResultString = summary
		jobObj.LastStatusUpdateTime = time.Now()
		return "Version Check Completed Successfully!", true
	}
	return "Job is running...", true
}
