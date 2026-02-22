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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cluster-director-mcp/cluster-director-gke-ai/pkg/config"
	"cluster-director-mcp/genericCore"

	"cloud.google.com/go/logging"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type SearchLogsRequest struct {
	ClusterName  string `json:"ClusterName"`
	NumberOfDays int    `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
}

type SearchLogsRequestWithoutCluster struct {
	NumberOfDays int `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
}

type SearchLogsRequestXidGkeClusters struct {
	StartDate   string `json:"StartDate,omitempty" jsonschema:"description=Start date to search Cloud Logs"`
	EndDate     string `json:"EndDate,omitempty" jsonschema:"description=End date to search Cloud Logs"`
	JobName     string `json:"JobName,omitempty" jsonschema:"description=GKE Job Name whose logs we should search"`
	ClusterName string `json:"ClusterName,omitempty" jsonschema:"description=GKE cluster Name whose logs we should search"`
	MaxResults  int    `json:"MaxResults,omitempty" jsonschema:"default=100,description=Maximum number of log entries to retrieve"`
}

type SearchLogsRequestXidGkePods struct {
	StartDate  string `json:"StartDate,omitempty" jsonschema:"description=Start date to search Cloud Logs"`
	EndDate    string `json:"EndDate,omitempty" jsonschema:"description=End date to search Cloud Logs"`
	JobName    string `json:"JobName,omitempty" jsonschema:"description=Name of the GKE Job whose logs we should search"`
	PodName    string `json:"PodName,omitempty" jsonschema:"description=Name of the pod whose logsr5 we should search"`
	MaxResults int    `json:"MaxResults,omitempty" jsonschema:"default=100,description=Maximum number of log entries to retrieve"`
}

type SearchLogsRequestXidGce struct {
	StartDate    string `json:"StartDate,omitempty" jsonschema:"description=Start date to search Cloud Logs"`
	EndDate      string `json:"EndDate,omitempty" jsonschema:"description=End date to search Cloud Logs"`
	NumberOfDays int    `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
	InstanceName string `json:"InstanceName,omitempty" jsonschema:"description=Name of the instance whose logs we should search"`
	JobName      string `json:"JobName,omitempty" jsonschema:"description=Name of the GKE Job whose logs we should search"`
	PodName      string `json:"PodName,omitempty" jsonschema:"description=Name of the pod whose logsr5 we should search"`
	MaxResults   int    `json:"MaxResults,omitempty" jsonschema:"default=100,description=Maximum number of log entries to retrieve"`
}

type SearchLogsResponse struct {
	Status string `json:"status"`
}

type SearchLogsRequestStockout struct {
	StartDate     string `json:"StartDate,omitempty" jsonschema:"description=Start date to search Cloud Logs"`
	EndDate       string `json:"EndDate,omitempty" jsonschema:"description=End date to search Cloud Logs"`
	NumberOfDays  int    `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
	ProjectID     string `json:"ProjectID,omitempty" jsonschema:"description=GCP Project ID. Optional if default is set."`
	ClusterFilter string `json:"ClusterFilter,omitempty" jsonschema:"description=Filter results by cluster type. Valid values: 'gke', 'slurm', or 'all' (default)."`
}

type CheckConsumptionRequest struct {
	InstanceNames []string `json:"InstanceNames" jsonschema:"description=List of GCE instance names to check"`
	Zone          string   `json:"Zone" jsonschema:"description=GCP Zone (e.g., us-central1-a). Optional: If omitted, the tool will search for the instances."`
	ProjectID     string   `json:"ProjectID,omitempty" jsonschema:"description=GCP Project ID. Optional if default is set."`
}

type handlers struct {
	c *config.Config
}

// LogSearchType is kept here because it is specific to GKE/Slurm domain logic
type LogSearchType int

const (
	AreNCCLDebugLogsEnabled                  LogSearchType = iota // 0
	WereThereNCCLWarnMessages                                     // 1
	WereThereNCCLErrorMessages                                    // 2
	WereThereXidFailureMessagesInGkeCluster                       // 3
	WereThereXidFailureMessagesInGkePod                           // 4
	WereThereXidFailureMessagesInGceInstance                      // 5
)

func Install(s *mcp.Server, c *config.Config) {
	h := &handlers{
		c: c,
	}

	genericCore.GetGCloudToken()
	go genericCore.GetGCloudRegionsAndZones(context.Background(), c.GetDefaultProjectID())
	genericCore.CreateScratchDir()

	searchXidGkeClusters := mcp.Tool{
		Name:        "search_xid_in_gke_clusters",
		Description: "Debug slowness in running job on a GCP GKE Cluster",
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
					"description": "The start of the time period to filter search results. Optional argument.",
				},
				"EndDate": map[string]interface{}{
					"type":        "string",
					"format":      "date",
					"description": "The end of the time period to filter search results. This is an optional argument.",
				},
				"JobName": map[string]interface{}{
					"type":        "string",
					"description": "GKE Job name whose logs to search. Required argument.",
				},
				"ClusterName": map[string]interface{}{
					"type":        "string",
					"description": "Name of the GKE cluster whose log to search. Required argument.",
				},
				"MaxResults": map[string]interface{}{
					"type":        "number",
					"description": "Maximum number of log entries to retrieve. Defaults to 100. Optional argument.",
				},
			},
			"required": []string{"ClusterName", "JobName"},
		},
	}
	mcp.AddTool(
		s,
		&searchXidGkeClusters,
		func(ctx context.Context, _ *mcp.CallToolRequest, req SearchLogsRequestXidGkeClusters) (*mcp.CallToolResult, SearchLogsResponse, error) {
			result, foundsIssues, err := h.searchLogsMCP(ctx, &req, WereThereXidFailureMessagesInGkeCluster)
			if foundsIssues {
				result += ". Your job is possibly slow because we found Xid errors on the nodes running it"
			}
			return nil, SearchLogsResponse{Status: result}, err
		},
	)

	checkConsumptionTool := mcp.Tool{
		Name:        "check_instance_consumption",
		Description: "Check if GKE (Kubernetes) cluster nodes are Spot, On-Demand, or consuming a Reservation. Use this for GKE clusters or GKE node pools.",
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
		Name:        "search_stockout_errors",
		Description: "were there any stock out/ stockout errors (during provisioning - optional) (ZONE_RESOURCE_POOL_EXHAUSTED) for GKE clusters.",
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
			},
			"required": []string{},
		},
	}
	mcp.AddTool(
		s,
		&searchStockoutErrorsTool,
		func(ctx context.Context, _ *mcp.CallToolRequest, req SearchLogsRequestStockout) (*mcp.CallToolResult, any, error) {
			result, err := genericCore.CheckStockoutErrorsCore(ctx, h.c.GetDefaultProjectID(), req.ProjectID, req.StartDate, req.EndDate, req.NumberOfDays, req.ClusterFilter)
			if err != nil {
				return nil, nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: result}}}, nil, nil
		},
	)

}

func (h *handlers) searchLogsMCP(ctx context.Context, request *SearchLogsRequestXidGkeClusters, searchType LogSearchType) (string, bool, error) {
	genericCore.WriteToLog(fmt.Sprintf("-------------------searchLogsMCP() invoked for Cluster: %s, Job: %s-------------------", request.ClusterName, request.JobName))

	projectID := h.c.GetDefaultProjectID()
	if projectID == "" {
		return "Could not determine GCP project. Please run: gcloud config set project \"your-project-name\" and restart the AI Assistant", false, nil
	}

	// Common configuration
	lookbackDuration := time.Duration(14) * 24 * time.Hour
	startTimeFromLookBack := time.Now().Add(-lookbackDuration).Format(time.RFC3339)

	limit := request.MaxResults
	if limit <= 0 {
		limit = 100
	}

	var filter string
	var processor genericCore.LogProcessor

	// Case-by-Case Refactoring: Define the filter and processor based on the search type
	switch searchType {
	case WereThereXidFailureMessagesInGkeCluster:
		if request.JobName == "" || request.ClusterName == "" {
			return "JobName and ClusterName are required", false, nil
		}
		filter = fmt.Sprintf(`resource.type="k8s_pod" AND jsonPayload.reason="Scheduled" AND resource.labels.cluster_name="%s" AND (resource.labels.pod_name:"%s" OR labels."k8s-pod/job-name"="%s")`, request.ClusterName, request.JobName, request.JobName)

		processor = func(entry *logging.Entry) ([]string, bool) {
			podName := entry.Resource.Labels["pod_name"]
			location := entry.Resource.Labels["location"]
			payload := genericCore.GetPayloadString(entry)
			// Returns structure: [podName, empty_instName, location, payload_for_xid]
			return []string{podName, "", location, payload}, true
		}

	case AreNCCLDebugLogsEnabled:
		filter = fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload="*NCCL*" OR jsonPayload.message="*NCCL*")`, startTimeFromLookBack)
		processor = func(entry *logging.Entry) ([]string, bool) {
			return []string{genericCore.GetPayloadString(entry)}, true
		}

	case WereThereNCCLWarnMessages:
		filter = fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload:"*NCCL WARN*" OR jsonPayload.message:"*NCCL WARN*")`, startTimeFromLookBack)
		processor = func(entry *logging.Entry) ([]string, bool) {
			return []string{genericCore.GetPayloadString(entry)}, true
		}

	case WereThereNCCLErrorMessages:
		filter = fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload:"*NCCL ERROR*" OR jsonPayload.message:"*NCCL ERROR*")`, startTimeFromLookBack)
		processor = func(entry *logging.Entry) ([]string, bool) {
			return []string{genericCore.GetPayloadString(entry)}, true
		}

	default:
		// Fallback for safety or unhandled types
		filter = fmt.Sprintf(`timestamp >= "%s"`, startTimeFromLookBack)
		processor = func(entry *logging.Entry) ([]string, bool) {
			return []string{genericCore.GetPayloadString(entry)}, true
		}
	}

	// Execute the decoupled search
	_, returnResults, logSearchSuccess := genericCore.SearchLogsCore(ctx, projectID, filter, limit, processor)

	// Post-processing and Return Formatting
	if !logSearchSuccess {
		switch searchType {
		case AreNCCLDebugLogsEnabled:
			return "NCCL Debug Info NOT found", false, nil
		case WereThereNCCLWarnMessages:
			return "NCCL WARN messages NOT found", false, nil
		case WereThereNCCLErrorMessages:
			return "NCCL ERROR messages  NOT found", false, nil
		default:
			return "No logs found matching the criteria", false, nil
		}
	}

	switch searchType {
	case WereThereXidFailureMessagesInGkeCluster:
		// Fill in GKE specific variables locally
		for i := range returnResults {
			if len(returnResults[i]) > 0 && returnResults[i][0] != "" {
				returnResults[i][1] = getGceInstanceForPod(returnResults[i][0])
			}
			if len(returnResults[i]) > 3 && returnResults[i][3] != "" {
				returnResults[i][3] = parseXidNumber(returnResults[i][3])
			}
		}

		retMesgStr := fmt.Sprintf("searchLogsMCP.4444 got %d results for Pods", len(returnResults))
		genericCore.WriteToLog("Success: " + retMesgStr + " but no Xid errors")

		if len(returnResults) != 0 {
			return doInstanceLogsHaveXidErrors(returnResults, h, ctx, projectID), true, nil
		}
		return doInstanceLogsHaveXidErrors(returnResults, h, ctx, projectID), false, nil

	case AreNCCLDebugLogsEnabled:
		return "NCCL Debug is enabled\n", false, nil
	case WereThereNCCLWarnMessages:
		return "NCCL WARN messages were found\n", false, nil
	case WereThereNCCLErrorMessages:
		return "NCCL ERROR messages found.\n", false, nil
	}

	return "Search completed successfully", false, nil
}

func doInstanceLogsHaveXidErrors(searchResults [][]string, h *handlers, ctx context.Context, projectID string) string {
	filter := ` resource.type="gce_instance"  `

	for _, arr := range searchResults {
		if len(arr) > 1 && arr[1] != "" {
			filter += fmt.Sprintf(` AND labels."compute.googleapis.com/resource_name"="%s" `, arr[1])
		}
	}

	processor := func(entry *logging.Entry) ([]string, bool) {
		return []string{genericCore.GetPayloadString(entry)}, true
	}

	_, xidResults, success := genericCore.SearchLogsCore(ctx, projectID, filter, 100, processor)

	if success {
		return fmt.Sprintf("Found %v Xid errors in %v GCE instances", len(xidResults), len(searchResults))
	}
	return "No Xid errors found on instances"
}

func getGceInstanceForPod(podName string) string {
	// TODO: Parameterize namespace instead of hardcoding
	namespace := "default"

	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	config, _ := clientcmd.BuildConfigFromFlags("", kubeconfig)
	clientset, _ := kubernetes.NewForConfig(config)

	pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Could not find instance name for pod %s. error: %v", podName, err))
		return ""
	}

	return pod.Spec.NodeName
}

func parseXidNumber(logLine string) string {
	_, after, found := strings.Cut(logLine, "): ")
	if found {
		numberStr, _, _ := strings.Cut(after, ",")
		return numberStr
	}
	return ""
}

func (h *handlers) checkConsumptionMCP(ctx context.Context, req CheckConsumptionRequest) (string, error) {
	return genericCore.ProcessConsumptionRequest(
		ctx,
		req.InstanceNames,
		req.Zone,
		req.ProjectID,
		h.c.GetDefaultProjectID(),
	)
}
