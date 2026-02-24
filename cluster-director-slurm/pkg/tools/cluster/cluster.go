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
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cluster-director-mcp/cluster-director-slurm/pkg/config"
	"cluster-director-mcp/genericCore"
	"cluster-director-mcp/persistence"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	compute "google.golang.org/api/compute/v0.alpha"
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

type SearchLogsRequestStockout struct {
	StartDate     string `json:"StartDate,omitempty" jsonschema:"description=Start date to search Cloud Logs"`
	EndDate       string `json:"EndDate,omitempty" jsonschema:"description=End date to search Cloud Logs"`
	NumberOfDays  int    `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
	ProjectID     string `json:"ProjectID,omitempty" jsonschema:"description=GCP Project ID. Optional if default is set."`
	ClusterFilter string `json:"ClusterFilter,omitempty" jsonschema:"description=Filter results by cluster type. Valid values: 'gke', 'slurm', or 'all' (default)."`
	ClusterName   string `json:"ClusterName,omitempty" jsonschema:"description=Filter results specifically for a named cluster. Optional."`
}

type handlers struct {
	c *config.Config
}

// CheckConsumptionRequest defines the structure for the tool input
type CheckConsumptionRequest struct {
	InstanceNames []string `json:"InstanceNames" jsonschema:"description=List of GCE instance names to check"`
	Zone          string   `json:"Zone" jsonschema:"description=GCP Zone (e.g., us-central1-a). Optional: If omitted, the tool will search for the instances."`
	ProjectID     string   `json:"ProjectID,omitempty" jsonschema:"description=GCP Project ID. Optional if default is set."`
}

func Install(s *mcp.Server, c *config.Config) {
	h := &handlers{
		c: c,
	}

	// sets authToken
	genericCore.GetGCloudToken()

	// HCS does NOT support ALL regions and has an API to return the list of
	// regions it supports. Use HCS' API instead of GCE API to get ALL regions
	// because the GCE API is an overkill
	go getAllRegionsAndZonesSupportedByHCS(c.GetDefaultProjectID())

	// A place where we keep temporary files
	genericCore.CreateScratchDir()
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
			result, err := h.listClustersMCP(ctx, &req)
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
			result, err := h.getClusterMCP(ctx, &req)
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
			result, err := h.showClusterStateMCP(ctx, &req)
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
			result, err := h.showJobStateMCP(ctx, &req)
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
			result, err := h.showRecentJobsMCP(ctx, &req)
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
			result, err := h.runNCCLTestsMCP(ctx, &req)
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
			result, err := h.runDCGMTestsMCP(ctx, &req)
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
			result, err := h.listPartitionInfoMCP(ctx, &req)
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
			result, err := h.checkCDMcpJobStatusMCP(ctx, &req)
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
			result, err := h.checkMaintenanceEventsMCP(ctx, &req)
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
			result, err := h.showClusterSoftwareVersionInfoMCP(ctx, &req)
			return nil, SoftwareVersionInfoResponse{VersionInfo: result}, err
		})

	// Check Instance Consumption Type
	checkConsumptionTool := mcp.Tool{
		Name:        "check_instance_consumption",
		Description: "Check if Slurm HPC cluster nodes (login or compute) are Spot, On-Demand, or consuming a Reservation. Use this for Slurm clusters.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"InstanceNames": map[string]interface{}{
					"type": "array",
					"items": map[string]interface{}{
						"type": "string",
					},
					"description": "List of GCE instance names",
				},
				"Zone": map[string]interface{}{
					"type":        "string",
					"description": "GCP Zone. Optional: If omitted, the tool will search for the instances.",
				},
				"ProjectID": map[string]interface{}{
					"type":        "string",
					"description": "GCP Project ID. Optional.",
				},
			},
			"required": []string{"InstanceNames"},
		},
	}
	mcp.AddTool(
		s,
		&checkConsumptionTool,
		func(ctx context.Context, _ *mcp.CallToolRequest, req CheckConsumptionRequest) (*mcp.CallToolResult, any, error) {
			result, err := h.checkConsumptionMCP(ctx, req)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		},
	)

	searchStockoutErrorsTool := mcp.Tool{
		Name:        "search_slurm_stockout_errors",
		Description: "were there any stock out/ stockout errors (during provisioning - optional) for slurm clusters.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"StartDate": map[string]interface{}{
					"type":        "string",
					"format":      "date",
					"description": "Start date to search Cloud Logs. Optional argument.",
				},
				"EndDate": map[string]interface{}{
					"type":        "string",
					"format":      "date",
					"description": "End date to search Cloud Logs. Optional argument.",
				},
				"NumberOfDays": map[string]interface{}{
					"type":        "number",
					"description": "Number of days before today to search Cloud Logs. Defaults to 14. Optional argument.",
				},
				"ProjectID": map[string]interface{}{
					"type":        "string",
					"description": "GCP Project ID. Optional if default is set.",
				},
				"ClusterFilter": map[string]interface{}{
					"type":        "string",
					"enum":        []string{"gke", "slurm", "all"},
					"description": "Filter results by cluster type. Provide 'gke' for GKE clusters, 'slurm' for Slurm clusters, or 'all'. Defaults to 'all'.",
				},
				"ClusterName": map[string]interface{}{
					"type":        "string",
					"description": "Filter results specifically for a named cluster natively. Optional argument.",
				},
			},
			"required": []string{},
		},
	}
	mcp.AddTool(
		s,
		&searchStockoutErrorsTool,
		func(ctx context.Context, _ *mcp.CallToolRequest, req SearchLogsRequestStockout) (*mcp.CallToolResult, any, error) {
			result, err := genericCore.CheckStockoutErrorsCore(ctx, h.c.GetDefaultProjectID(), req.ProjectID, req.StartDate, req.EndDate, req.NumberOfDays, req.ClusterFilter, req.ClusterName)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		},
	)

}

func (h *handlers) listClustersCore(projectID string) (string, error) {
	genericCore.WriteToLog("-------------------listClustersCore()-------------------")

	clusterListString, _ := getClustersInAllRegions(projectID)
	return clusterListString, nil
}

func (h *handlers) listClustersMCP(ctx context.Context, request *ListClustersRequest) (string, error) {
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}
	genericCore.WriteToLog("-------------------listClustersMCP()-------------------")
	genericCore.WriteToLog("projectId : " + projectID)
	return h.listClustersCore(projectID)
}

func (h *handlers) getClusterCore(projectID string, clusterName string) (string, error) {
	genericCore.WriteToLog("-------------------getClusterCore()-------------------")

	getClustersInAllRegions(h.c.GetDefaultProjectID())
	if clusterJSON, ok := clusterNames2JSON[clusterName]; ok {
		return clusterJSON, nil
	} else {
		return fmt.Sprintf("Could not get information on cluster %s in project %s", clusterName, projectID), nil
	}
}

func (h *handlers) getClusterMCP(ctx context.Context, request *GetClusterRequest) (string, error) {
	clusterName := request.ClusterName
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}
	genericCore.WriteToLog("-------------------getClusterMCP()-------------------")
	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("clusterName : " + clusterName)

	return h.getClusterCore(projectID, clusterName)
}

func (h *handlers) checkMaintenanceEventsCore(projectID string, zone string, clusterName string) (string, error) {
	genericCore.WriteToLog("-------------------checkMaintenanceEventsCore()-------------------")

	nodeList, success := getComputeNodesInCluster(clusterName+"-login-001", zone, projectID)
	if !success {
		return fmt.Sprintf("Could not get nodes in cluster %s in project %s", clusterName, projectID), nil
	}
	ctx := context.Background()
	computeService, err := compute.NewService(ctx)
	if err != nil {
		return fmt.Sprintf("Failed to create compute service: %v", err), nil
	}
	returnStr := ""
	for _, node := range nodeList {
		instance, err := computeService.Instances.Get(projectID, zone, node).Do()
		returnStr += "Maintenance info for node " + node + " : "
		if err != nil {
			returnStr += fmt.Sprintf("Could not get maintenance info: %v\n", err)
			continue
		}
		// Native scheduling inspection
		if instance.Scheduling != nil && instance.Scheduling.OnHostMaintenance != "" {
			returnStr += fmt.Sprintf("OnHostMaintenance: %s\n", instance.Scheduling.OnHostMaintenance)
		}
		if instance.Scheduling != nil && instance.Scheduling.Preemptible {
			returnStr += "Preemptible: true\n"
		}
		if instance.Status != "" {
			returnStr += fmt.Sprintf("Instance Status: %s\n", instance.Status)
		}
		if instance.Scheduling == nil {
			returnStr += "No scheduling info available\n"
		}
		returnStr += "\n"
	}
	return returnStr, nil
}

func (h *handlers) checkMaintenanceEventsMCP(ctx context.Context, request *MaintenanceEventsRequest) (string, error) {
	clusterName := request.ClusterName
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}

	genericCore.WriteToLog("-------------------checkMaintenanceEventsMCP()-------------------")
	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("clusterName : " + clusterName)

	zone := getZoneForCluster(projectID, clusterName)
	if zone == "" {
		return fmt.Sprintf("Could not get zone for cluster %s in project %s", clusterName, projectID), nil
	}
	return h.checkMaintenanceEventsCore(projectID, zone, clusterName)
}

func (h *handlers) showClusterSoftwareVersionInfoCore(projectID string, zone string, clusterName string) (string, error) {
	genericCore.WriteToLog("-------------------showClusterSoftwareVersionInfoCore (Async Refactor)-------------------")

	// 2. Check for recent jobs (Rate Limiting)
	operationSuccessful, operationMesg, recentJob := checkIfLongRunningJobsSubmittedRecently(versionCheckRetryWindow, projectID)
	if !operationSuccessful {
		return operationMesg, nil
	}
	if recentJob {
		return fmt.Sprintf("A job was submitted recently. Please wait %s  before running version checks again.", versionCheckRetryWindow), nil
	}

	loginNode := clusterName + "-login-001"

	// 3. Verify Login Node Access
	sshOut, success := genericCore.RunSSHOnNode(loginNode, projectID, zone, "echo SUCCESS")
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

	localScriptName := genericCore.LOCAL_HOST_SCRATCH_DIR + "/version_check.sbatch"
	file, err := os.Create(localScriptName)
	if err != nil {
		return "Failed to create local script: " + err.Error(), nil
	}
	for _, line := range slurmScriptContent {
		fmt.Fprintln(file, line)
	}
	file.Close()

	// 7. Deploy to Cluster
	genericCore.RunSSHOnNode(loginNode, projectID, zone, "rm -rf "+jobObj.RunDir+"; mkdir -p "+jobObj.RunDir)

	remoteScriptPath := jobObj.RunDir + "/version_check.sbatch"

	sshOut, success = genericCore.RunSCP(projectID, zone, localScriptName, loginNode+":"+remoteScriptPath)
	if !success {
		return "Failed to copy version check script to login node.", nil
	}

	// 8. Submit the Job
	submitCmd := fmt.Sprintf("cd %s && sbatch version_check.sbatch", jobObj.RunDir)
	sshOut, success = genericCore.RunSSHOnNode(loginNode, projectID, zone, submitCmd)
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

func (h *handlers) showClusterSoftwareVersionInfoMCP(ctx context.Context, request *SoftwareVersionInfoRequest) (string, error) {
	genericCore.WriteToLog("-------------------showClusterSoftwareVersionInfoMCP (Async Refactor)-------------------")

	// 1. Setup variables from Request Struct
	clusterName := request.ClusterName
	projectID := request.ProjectID
	if projectID == "" {
		projectID = h.c.GetDefaultProjectID()
	}

	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("clusterName : " + clusterName)

	zone := getZoneForCluster(projectID, clusterName)
	if zone == "" {
		return fmt.Sprintf("Could not get zone for cluster %s in project %s", clusterName, projectID), nil
	}
	genericCore.WriteToLog("zone : " + zone)

	return h.showClusterSoftwareVersionInfoCore(projectID, zone, clusterName)
}

func getComputeNodesInCluster(loginNode string, zone string, projectId string) ([]string, bool) {
	var returnArr []string
	sshOut, success := genericCore.RunSSHOnNode(loginNode, projectId, zone, "sinfo -N -l")
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

func (h *handlers) showClusterStateMCP(ctx context.Context, request *ShowClusterStateRequest) (string, error) {
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
	sshOut, success := genericCore.RunSSHOnNode(clusterName+"-login-001", projectId, zone, "sinfo")
	return sshOut, success
}

func (h *handlers) showRecentJobsCore(projectID string, zone string, clusterName string) (string, error) {
	genericCore.WriteToLog("-------------------showRecentJobsCore()-------------------")

	loginNode := clusterName + "-login-001"
	sshOut, success := genericCore.RunSSHOnNode(loginNode, projectID, zone, "sacct")
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not get recent jobs!", nil
	}
	return sshOut, nil
}

func (h *handlers) showRecentJobsMCP(ctx context.Context, request *ShowRecentJobsRequest) (string, error) {
	genericCore.WriteToLog("-------------------showRecentJobsMCP()-------------------")
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

	return h.showRecentJobsCore(projectID, zone, clusterName)
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
	sshOut, success := genericCore.RunSSHOnNode(loginNode, projectID, zone, "echo SUCCESS")
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
	localScriptName := genericCore.LOCAL_HOST_SCRATCH_DIR + "/" + persistence.CDMCP_SHELL_SCRIPT_NAME
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
	sshOut, success = genericCore.RunSSHOnNode(loginNode, projectID, zone, "rm -rf "+jobObj.RunDir+"; mkdir -p "+jobObj.RunDir)
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not create dir " + jobObj.RunDir + " on login node " + loginNode + " in dir ", nil
	}
	sshOut, success = genericCore.RunSCP(projectID, zone, localScriptName, loginNode+":"+jobObj.RunScriptName)
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not copy " + persistence.CDMCP_SHELL_SCRIPT_NAME + " over to login node " + loginNode + " to location " + jobObj.RunScriptName, nil
	}

	sshOut, success = genericCore.RunSSHOnNode(loginNode, projectID, zone, "chmod +x "+jobObj.RunScriptName)
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not run \"chmod +x " + persistence.CDMCP_SHELL_SCRIPT_NAME + "\" on login node " + loginNode + " in dir " + jobObj.RunDir, nil
	}

	// Ignore return status
	_, _ = genericCore.RunSSHOnNode(loginNode, projectID, zone, "rm -f "+jobObj.RunDir+"/log.cluster-director-mcp_test")

	// Spawn a go routine - so we can run this asynchronously and return to the user
	go genericCore.RunSSHOnNode(loginNode, projectID, zone, "cd "+jobObj.RunDir+"; ./"+persistence.CDMCP_SHELL_SCRIPT_NAME+" 2>&1 > log.cluster-director-mcp_test")

	// Sleep 10 seconds, prevent check_job_status being called too soon
	time.Sleep(10 * time.Second)

	persistence.AppendNewJobDataAndWriteJobDataToDisk(jobObj)

	// Temporary comment end
	return testName + " tests running. Use check_job_status to get latest status on long running jobs", nil
}

func (h *handlers) runNCCLTestsMCP(ctx context.Context, request *RunClusterTestsRequest) (string, error) {
	return runNCCLOrDCGMTestsCore(h, ctx, request, persistence.NCCL_TEST)
}

// Returns a boolean to report probing job status - NOT status of job
// A value of true means, job status could be determined, false means
// it could not determine the status of the job s
func getNCCLOrDCGMTestsStatus(projectID string, ncclOrDCGMTestJobObj *persistence.LongRunningJob) (string, bool) {

	ncclOrDCGMTestJobObj.LastStatusCheckTime = time.Now()

	mainLogFileLocalPath := genericCore.LOCAL_HOST_SCRATCH_DIR + "/" + persistence.CDMCP_FULL_LOG

	// Remove local copy of MAIN log
	if genericCore.CheckFileOrDirExists(mainLogFileLocalPath, false) && !genericCore.DeleteFile(mainLogFileLocalPath) {
		genericCore.WriteToLog(fmt.Sprintf("Could not delet local copy of main log file: %s", mainLogFileLocalPath))
		return "Could not delete logfile " + mainLogFileLocalPath + "  . Check job status later or verify status manually.", false
	}

	// Remove local copy of SUMMARY log
	summaryLogFileLocalFullPath := genericCore.LOCAL_HOST_SCRATCH_DIR + "/" + persistence.CDMCP_SUMMARY_LOG
	if genericCore.CheckFileOrDirExists(summaryLogFileLocalFullPath, false) && !genericCore.DeleteFile(summaryLogFileLocalFullPath) {
		genericCore.WriteToLog(fmt.Sprintf("Could not delete local copy of summary log file: %s", summaryLogFileLocalFullPath))
		return "Could not delete logfile " + summaryLogFileLocalFullPath + "  . Check job status later or verify status manually.", false
	}

	// SCP over MAIN log
	sshOut, success := genericCore.RunSCP(projectID,
		ncclOrDCGMTestJobObj.Zone,
		ncclOrDCGMTestJobObj.LoginNodeName+":"+ncclOrDCGMTestJobObj.FullLogFilePath, mainLogFileLocalPath)
	if !success {
		persistence.WriteAllJobData()

		return string(genericCore.GetLastLines(sshOut, 10) + "\nNCCL/DCGM Test/Job is probably still running. I could not copy main logfile " + ncclOrDCGMTestJobObj.FullLogFilePath + " on cluster " + ncclOrDCGMTestJobObj.ClusterName + " over from login node " + ncclOrDCGMTestJobObj.LoginNodeName + "  . Check job status later or verify status manually."), false
	}

	// Read MAIN log
	mainLogContents, _ := genericCore.SlurpFile(mainLogFileLocalPath)

	// SCP over SUMMARY log
	sshOut, success = genericCore.RunSCP(projectID,
		ncclOrDCGMTestJobObj.Zone,
		ncclOrDCGMTestJobObj.LoginNodeName+":"+ncclOrDCGMTestJobObj.SummaryLogFilePath, summaryLogFileLocalFullPath)
	if success {
		ncclOrDCGMTestJobObj.JobExecutionResultString, _ = genericCore.SlurpFile(summaryLogFileLocalFullPath)
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

func (h *handlers) checkCDMcpJobStatusMCP(ctx context.Context, request *CheckCDMcpJobStatusRequest) (string, error) {
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

func (h *handlers) runDCGMTestsMCP(ctx context.Context, request *RunClusterTestsRequest) (string, error) {
	genericCore.WriteToLog("-------------------runDCGMTests()-------------------")
	return runNCCLOrDCGMTestsCore(h, ctx, request, persistence.DCGM_TEST)
}

func (h *handlers) showJobStateCore(projectID string, zone string, clusterName string) (string, error) {
	loginNode := clusterName + "-login-001"
	sshOut, success := genericCore.RunSSHOnNode(loginNode, projectID, zone, "squeue")
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not run squeue to figure out job state on login node " + loginNode + " on cluster " + clusterName + " project " + projectID, nil
	}

	return sshOut, nil
}

func (h *handlers) showJobStateMCP(ctx context.Context, request *ShowJobStateRequest) (string, error) {
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

	return h.showJobStateCore(projectID, zone, clusterName)
}

func (h *handlers) listPartitionInfoCore(projectID string, zone string, clusterName string) (string, error) {
	loginNode := clusterName + "-login-001"
	sshOut, success := genericCore.RunSSHOnNode(loginNode, projectID, zone, "scontrol show partition")
	if !success {
		return genericCore.GetLastLines(sshOut, 10) + "\nCould not run scontrol on login node " + loginNode + " to figure out partition information", nil
	}

	return sshOut, nil
}

func (h *handlers) listPartitionInfoMCP(ctx context.Context, request *ListPartitionInfoRequest) (string, error) {
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

	genericCore.WriteToLog("-------------------listPartitionInfoMCP()-------------------")
	genericCore.WriteToLog("projectId : " + projectID)
	genericCore.WriteToLog("zone : " + zone)
	genericCore.WriteToLog("clusterName : " + clusterName)

	return h.listPartitionInfoCore(projectID, zone, clusterName)

}

// Helper to check status specifically for Version Check jobs
func getVersionCheckStatus(projectID string, jobObj *persistence.LongRunningJob) (string, bool) {
	jobObj.LastStatusCheckTime = time.Now()

	localLogPath := genericCore.LOCAL_HOST_SCRATCH_DIR + "/" + persistence.CDMCP_FULL_LOG

	// Clean up stale local logs before fetching new ones
	if genericCore.CheckFileOrDirExists(localLogPath, false) {
		genericCore.DeleteFile(localLogPath)
	}

	// Fetch the log file generated by sbatch (e.g., version_check_123.log)
	_, success := genericCore.RunSCP(projectID, jobObj.Zone, jobObj.LoginNodeName+":"+jobObj.FullLogFilePath, localLogPath)

	if !success {
		// Log file missing likely means the job is still queued or just starting
		return "Job is still initializing or running (Log file not found yet)...", false
	}

	content, err := genericCore.SlurpFile(localLogPath)
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

// Implementation
func (h *handlers) checkConsumptionMCP(ctx context.Context, req CheckConsumptionRequest) (string, error) {
	return genericCore.ProcessConsumptionRequest(
		ctx,
		req.InstanceNames,
		req.Zone,
		req.ProjectID,
		h.c.GetDefaultProjectID(),
	)
}
