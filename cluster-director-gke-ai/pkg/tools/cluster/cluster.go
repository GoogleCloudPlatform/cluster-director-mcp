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
	"cloud.google.com/go/logging/logadmin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"
)

type SearchLogsRequest struct {
	ClusterName  string `json:"ClusterName"`
	NumberOfDays int    `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
}

type SearchLogsRequestWithoutCluster struct {
	NumberOfDays int `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
}

type SearchLogsRequestXidGkeClusters struct {
	StartDate string `json:"StartDate,omitempty" jsonschema:"description=Start date to search Cloud Logs"`
	EndDate   string `json:"EndDate,omitempty" jsonschema:"description=End date to search Cloud Logs"`
	JobName     string `json:"JobName,omitempty" jsonschema:"description=GKE Job Name whose logs we should search"`
	ClusterName string `json:"ClusterName,omitempty" jsonschema:"description=GKE cluster Name whose logs we should search"`
}

type SearchLogsRequestXidGkePods struct {
	StartDate string `json:"StartDate,omitempty" jsonschema:"description=Start date to search Cloud Logs"`
	EndDate   string `json:"EndDate,omitempty" jsonschema:"description=End date to search Cloud Logs"`
	JobName   string `json:"JobName,omitempty" jsonschema:"description=Name of the GKE Job whose logs we should search"`
	PodName   string `json:"PodName,omitempty" jsonschema:"description=Name of the pod whose logsr5 we should search"`
}

type SearchLogsRequestXidGce struct {
	StartDate    string `json:"StartDate,omitempty" jsonschema:"description=Start date to search Cloud Logs"`
	EndDate      string `json:"EndDate,omitempty" jsonschema:"description=End date to search Cloud Logs"`
	NumberOfDays int    `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
	InstanceName string `json:"InstanceName,omitempty" jsonschema:"description=Name of the instance whose logs we should search"`
	JobName      string `json:"JobName,omitempty" jsonschema:"description=Name of the GKE Job whose logs we should search"`
	PodName      string `json:"PodName,omitempty" jsonschema:"description=Name of the pod whose logsr5 we should search"`
}

type SearchLogsResponse struct {
	Status string `json:"status"`
}

// CheckConsumptionRequest defines the structure for the tool input
type CheckConsumptionRequest struct {
	InstanceNames []string `json:"InstanceNames" jsonschema:"description=List of GCE instance names to check"`
	Zone          string   `json:"Zone" jsonschema:"description=GCP Zone (e.g., us-central1-a). Optional: If omitted, the tool will search for the instances."`
	ProjectID     string   `json:"ProjectID,omitempty" jsonschema:"description=GCP Project ID. Optional if default is set."`
}

type handlers struct {
	c *config.Config
}

type LogSearchType int

const (
	AreNCCLDebugLogsEnabled                  LogSearchType = iota // 0
	WereThereNCCLWarnMessages                                     // 1
	WereThereNCCLErrorMessages                                    // 2
	WereThereXidFailureMessagesInGkeCluster                       // 3
	WereThereXidFailureMessagesInGkePod                           // 3
	WereThereXidFailureMessagesInGceInstance                      // 3
)

func Install(s *mcp.Server, c *config.Config) {
	h := &handlers{
		c: c,
	}

	// sets authToken
	getGCloudToken()

	// A place where we keep temporary files
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

	// Check Instance Consumption Type
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

}

func (h *handlers) searchLogsMCP(ctx context.Context, request *SearchLogsRequestXidGkeClusters, searchType LogSearchType) (string, bool, error) {
	projectID := h.c.GetDefaultProjectID()
	if projectID == "" {
		return "Could not determine GCP project. Please run: gcloud config set project \"your-project-name\" and restart the AI Assistant", false, nil
	}

	clusterName := ""
	var startDate, endDate time.Time
	var startDateStr, endDateStr, instanceName, jobName, podName string
	var numberOfDays int
	var startDateValid, endDateValid bool

	if searchType == WereThereXidFailureMessagesInGkeCluster {

		startDateStr = request.StartDate
		startDate, startDateValid = genericCore.ParseTime(startDateStr)

		endDateStr = request.EndDate
		endDate, endDateValid = genericCore.ParseTime(endDateStr)

		jobName = request.JobName
		clusterName = request.ClusterName

		if jobName == "" {
			return "JobName is required", false, nil
		}
		if clusterName == "" {
			return "ClusterName is required", false, nil
		}
	}

	lookbackDuration := time.Duration(numberOfDays) * 24 * time.Hour // How far back to look

	// Build the filter for GKE logs
	// We look for 'k8s_container' resources.
	// We specifically filter for the string "NCCL" to reduce the data we fetch.
	startTimeFromLookBack := time.Now().Add(-lookbackDuration).Format(time.RFC3339)
	filter := ""

	if searchType == AreNCCLDebugLogsEnabled {
		filter = fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload="*NCCL*" OR jsonPayload.message="*NCCL*")`, startTimeFromLookBack)
	} else if searchType == WereThereNCCLWarnMessages {
		filter = fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload:"*NCCL WARN*" OR jsonPayload.message:"*NCCL WARN*")`, startTimeFromLookBack)
	} else if searchType == WereThereNCCLErrorMessages {
		filter = fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload:"*NCCL ERROR*" OR jsonPayload.message:"*NCCL ERROR*")`, startTimeFromLookBack)
	} else if searchType == WereThereXidFailureMessagesInGkeCluster {
		filter = `resource.type="k8s_pod" AND jsonPayload.reason="Scheduled" `
		filter += fmt.Sprintf(` AND resource.labels.cluster_name="%s" `, clusterName)
		filter += fmt.Sprintf(` AND (resource.labels.pod_name:"%s" OR labels."k8s-pod/job-name"="%s") `, jobName, jobName)
	} else if searchType == WereThereXidFailureMessagesInGkePod {
		filter = fmt.Sprintf(`(textPayload:"NVRM: Xid" OR jsonPayload.message:"NVRM: Xid") `)
		if instanceName != "" {
			//filter += fmt.Sprintf(` AND resource.type="gce_instance" AND resource.labels.instance_id="%s" `, instanceName)
			filter += fmt.Sprintf(` AND resource.type="gce_instance" AND labels."compute.googleapis.com/resource_name"="%s" `, instanceName)
		} else if podName != "" || jobName != "" {
			filter += ` resource.type="k8s_container" `
			if jobName != "" {
				filter += fmt.Sprintf(` AND (labels."k8s-pod/job-name="%s"" OR resource.labels.pod_name:"%s-") `, jobName, jobName)
			}
			//
			if podName != "" {
				filter += fmt.Sprintf(` AND resource.labels.pod_name="%s"" `, podName)
			}

			if startDateValid {
				filter += fmt.Sprintf(` AND timestamp >= "%s" `, startDate.Format("2006-01-02"))
			}
			if endDateValid {
				filter += fmt.Sprintf(` AND timestamp <= "%s" `, endDate.Format("2006-01-02"))
			}
			if !startDateValid && !endDateValid {
				filter += fmt.Sprintf(` AND timestamp >= "%s" `, startTimeFromLookBack)
			}
			if clusterName != "" {
				filter += fmt.Sprintf(` AND resource.labels.cluster_name="%s"`, clusterName)
			}
			return "Success", false, nil
		}
	}

	// hard coded 128 max results of now
	retMesgStr, returnResults, logSearchSuccess := searchLogsCore(h, ctx, projectID, filter, 128, searchType)

	if !logSearchSuccess {
		return retMesgStr, false, nil
	}

	if searchType == WereThereXidFailureMessagesInGkeCluster {
		if logSearchSuccess {
			// Each array in returnResults has podName, instName, location, xidErr
			retMesgStr = fmt.Sprintf("searchLogsMCP.4444 got %d results for Pods", len(returnResults))
			genericCore.WriteToLog("Success: " + retMesgStr + " but no Xid errors")

			if len(returnResults) != 0 {
				return doInstanceLogsHaveXidErrors(returnResults, h, ctx, projectID), true, nil
			} else {
				return doInstanceLogsHaveXidErrors(returnResults, h, ctx, projectID), false, nil
			}
		}
	} else if searchType == AreNCCLDebugLogsEnabled {
		if logSearchSuccess {
			retMesgStr += "NCCL Debug is enabled\n"
		} else {
			retMesgStr = "NCCL Debug Info NOT found"
		}
	} else if searchType == WereThereNCCLWarnMessages {
		if logSearchSuccess {
			retMesgStr += "NCCL WARN messages were found\n"
		} else {
			retMesgStr = "NCCL WARN messages NOT found"
		}
	} else if searchType == WereThereNCCLErrorMessages {
		if logSearchSuccess {
			retMesgStr += "NCCL ERROR messages found.\n"
		} else {
			retMesgStr = "NCCL ERROR messages  NOT found"
		}
	}

	return retMesgStr, false, nil
}

func doInstanceLogsHaveXidErrors(searchResults [][]string, h *handlers, ctx context.Context, projectID string) string {

	filter := ` resource.type="gce_instance"  `

	for _, arr := range searchResults {
		// podName, instName, location, XidErr
		if arr[1] != "" {
			filter += fmt.Sprintf(` AND labels."compute.googleapis.com/resource_name"="%s" `, arr[1])
		}
	}

	resultStr, xidResults, success := searchLogsCore(h, ctx, projectID, filter, 1, WereThereXidFailureMessagesInGceInstance)

	if success {
		t := fmt.Sprintf("Found %v Xid errors in %v GCE instances", len(xidResults), len(searchResults))
		return t
	} else {
		return resultStr
	}
}

func searchLogsCore(h *handlers, ctx context.Context, projectID string, filter string, maxResults int, searchType LogSearchType) (string, [][]string, bool) {
	genericCore.WriteToLog("-------------------searchLogsCore()-------------------")

	client, err := logadmin.NewClient(ctx, projectID)
	defer client.Close()
	if err != nil {
		genericCore.WriteToLog("Could not create logging client")
		return fmt.Sprintf("Could not create logging client: %v", err), nil, false
	}

	it := client.Entries(ctx, logadmin.Filter(filter))

	countResults := 0
	// This tells the GCP Server "only send me 1 item per page"
	//it.PageInfo().MaxSize = 1

	var searchResults [][]string
	//searchSuccess := true
	// Iterate through the logs
	var podName, payload, location, xidErr, instName string

	for {
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			//searchSuccess = false
			return fmt.Sprintf("Could not iterate over search results: %v", err), nil, false
		}

		payload = getPayloadString(entry)
		podName = entry.Resource.Labels["pod_name"]
		location = entry.Resource.Labels["location"]

		if searchType == WereThereXidFailureMessagesInGkeCluster {
			xidErr = parseXidNumber(payload)
			instName = getGceInstanceForPod(podName)
			searchResults = append(searchResults, []string{podName, instName, location, xidErr})
		} else {
			searchResults = append(searchResults, []string{payload})
		}

		countResults++
		if countResults > maxResults {
			break // Found positive confirmation, stop scanning
		}
	}
	return "Found Xid Errors", searchResults, true
}

func getGceInstanceForPod(podName string) string {
	// hard coded fix later
	namespace := "default"

	// Setup Kubernetes client
	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	config, _ := clientcmd.BuildConfigFromFlags("", kubeconfig)
	clientset, _ := kubernetes.NewForConfig(config)

	// 1. Get the Pod object
	pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		genericCore.WriteToLog("Could not find instance name for pod " + podName + " . error message: " + fmt.Sprintf("%v", err))
		return ""
	}

	nodeName := pod.Spec.NodeName

	// The ProviderID format is: gce://project-id/zone/instance-name
	//fmt.Printf("GCE Provider ID: %s\n", node.Spec.ProviderID)
	return nodeName
}

func parseXidNumber(logLine string) string {
	//logLine := "Jan 14 22:15:17 xxxxxx-nodeset1-40 kernel: [2437042.862382] NVRM: Xid (PCI:0000:84:00): 95, Uncontained: FBHUB. RST: Yes"

	// 1. Find the anchor "): "
	// "after" will be "95, Uncontained: FBHUB. RST: Yes"
	_, after, found := strings.Cut(logLine, "): ")

	if found {
		// 2. Cut at the comma to get just the number
		// "numberStr" will be "95"
		numberStr, _, _ := strings.Cut(after, ",")

		// 3. Convert to int
		//xid, _ := strconv.Atoi(strings.TrimSpace(numberStr))

		return numberStr
	}
	return ""
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
