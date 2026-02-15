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
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"cluster-director-mcp/cluster-director-gke-ai/pkg/config"
	"cluster-director-mcp/genericCore"
	"cluster-director-mcp/persistence"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"
)

var sbatchJobIDRegex = regexp.MustCompile(`Submitted batch job (\d+)`)

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
	//	NumberOfDays int    `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
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
	createScratchDir()
	/*
		ncclDebugEnabledTool := mcp.Tool{
			Name:        "are_nccl_debug_logs_enabled",
			Description: "Check if NCCL logs are enabled",
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:   true,
				IdempotentHint: true,
			},
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"ClusterName": map[string]interface{}{
						"type":        "string",
						"description": "Cluster name. Do not select it yourself, make sure the user provides or confirms the cluster name.",
					},
					"NumberOfDays": map[string]interface{}{
						"type":        "number",
						"description": "Number of days. Default value is 1",
					},
				},
				"required": []string{"ClusterName"},
			},
		}
		mcp.AddTool(
			s,
			&ncclDebugEnabledTool,
			func(ctx context.Context, _ *mcp.CallToolRequest, req SearchLogsRequest) (*mcp.CallToolResult, SearchLogsResponse, error) {
				result, err := h.searchLogsMCP(ctx, &req, AreNCCLDebugLogsEnabled)
				return nil, SearchLogsResponse{Status: result}, err
			},
		)

		ncclWarningsPresentTool := mcp.Tool{
			Name:        "are_nccl_warnings_present",
			Description: "Check if NCCL logs have warning messages",
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:   true,
				IdempotentHint: true,
			},
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"ClusterName": map[string]interface{}{
						"type":        "string",
						"description": "Cluster name. Do not select it yourself, make sure the user provides or confirms the cluster name.",
					},
					"NumberOfDays": map[string]interface{}{
						"type":        "number",
						"description": "Number of days. Default value is 1",
					},
				},
				"required": []string{"ClusterName"},
			},
		}
		mcp.AddTool(
			s,
			&ncclWarningsPresentTool,
			func(ctx context.Context, _ *mcp.CallToolRequest, req SearchLogsRequest) (*mcp.CallToolResult, SearchLogsResponse, error) {
				result, err := h.searchLogsMCP(ctx, &req, WereThereNCCLWarnMessages)
				return nil, SearchLogsResponse{Status: result}, err
			},
		)

		ncclErrorPresentTool := mcp.Tool{
			Name:        "are_nccl_errors_present",
			Description: "Check if NCCL logs have error messages",
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:   true,
				IdempotentHint: true,
			},
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"ClusterName": map[string]interface{}{
						"type":        "string",
						"description": "Cluster name. Do not select it yourself, make sure the user provides or confirms the cluster name.",
					},
					"NumberOfDays": map[string]interface{}{
						"type":        "number",
						"description": "Number of days. Default value is 1",
					},
				},
				"required": []string{"ClusterName"},
			},
		}
		mcp.AddTool(
			s,
			&ncclErrorPresentTool,
			func(ctx context.Context, _ *mcp.CallToolRequest, req SearchLogsRequest) (*mcp.CallToolResult, SearchLogsResponse, error) {
				result, err := h.searchLogsMCP(ctx, &req, WereThereNCCLErrorMessages)
				return nil, SearchLogsResponse{Status: result}, err
			},
		)
	*/

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
				//				"NumberOfDays": map[string]interface{}{
				//					"type":        "number",
				//					"description": "Number of days before today to search results. Default value is 14. Optional argument.",
				//				},
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

// func (h *handlers) searchLogsMCP(ctx context.Context, request *SearchLogsRequest, searchType LogSearchType) (string, error) {
func (h *handlers) searchLogsMCP(ctx context.Context, request *SearchLogsRequestXidGkeClusters, searchType LogSearchType) (string, bool, error) {
	genericCore.WriteToLog("searchLogsMCP.0000")
	projectID := h.c.GetDefaultProjectID()
	if projectID == "" {
		return "Could not determine GCP project. Please run: gcloud config set project \"your-project-name\" and restart the AI Assistant", false, nil
	}

	genericCore.WriteToLog("searchLogsMCP.0000.AAAA")

	clusterName := ""
	var startDate, endDate time.Time
	var startDateStr, endDateStr, instanceName, jobName, podName string
	var numberOfDays int
	var startDateValid, endDateValid bool

	if searchType == WereThereXidFailureMessagesInGkeCluster {
		genericCore.WriteToLog("searchLogsMCP.1111.AAAA")

		startDateStr = request.StartDate
		startDate, startDateValid = genericCore.ParseTime(startDateStr)

		endDateStr = request.EndDate
		endDate, endDateValid = genericCore.ParseTime(endDateStr)

		//		numberOfDays = request.NumberOfDays
		//		if numberOfDays == 0 {
		//			genericCore.WriteToLog("searchLogsMCP.1111.BBBB")
		//			numberOfDays = 14
		//		}

		jobName = request.JobName
		clusterName = request.ClusterName

		if jobName == "" {
			genericCore.WriteToLog("searchLogsMCP.1111.CCCC")
			return "JobName is required", false, nil
		}
		if clusterName == "" {
			genericCore.WriteToLog("searchLogsMCP.1111.DDDD")
			return "ClusterName is required", false, nil
		}
	}

	genericCore.WriteToLog("searchLogsMCP.2222.AAAA")

	lookbackDuration := time.Duration(numberOfDays) * 24 * time.Hour // How far back to look

	// Build the filter for GKE logs
	// We look for 'k8s_container' resources.
	// We specifically filter for the string "NCCL" to reduce the data we fetch.
	startTimeFromLookBack := time.Now().Add(-lookbackDuration).Format(time.RFC3339)
	filter := ""

	if searchType == AreNCCLDebugLogsEnabled {
		genericCore.WriteToLog("searchLogsMCP.2222.BBBB AreNCCLDebugLogsEnabled")
		filter = fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload="*NCCL*" OR jsonPayload.message="*NCCL*")`, startTimeFromLookBack)
	} else if searchType == WereThereNCCLWarnMessages {
		genericCore.WriteToLog("searchLogsMCP.2222.CCCC WereThereNCCLWarnMessages")
		filter = fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload:"*NCCL WARN*" OR jsonPayload.message:"*NCCL WARN*")`, startTimeFromLookBack)
	} else if searchType == WereThereNCCLErrorMessages {
		genericCore.WriteToLog("searchLogsMCP.2222.DDDD WereThereNCCLErrorMessages")
		filter = fmt.Sprintf(`resource.type="k8s_container" AND timestamp >= "%s" AND (textPayload:"*NCCL ERROR*" OR jsonPayload.message:"*NCCL ERROR*")`, startTimeFromLookBack)
	} else if searchType == WereThereXidFailureMessagesInGkeCluster {
		genericCore.WriteToLog("searchLogsMCP.2222.EEEE WereThereXidFailureMessagesInGkeCluster")
		filter = `resource.type="k8s_pod" AND jsonPayload.reason="Scheduled" `
		filter += fmt.Sprintf(` AND resource.labels.cluster_name="%s" `, clusterName)
		filter += fmt.Sprintf(` AND (resource.labels.pod_name:"%s" OR labels."k8s-pod/job-name"="%s") `, jobName, jobName)

		// Remove later
		genericCore.WriteToLog("searchLogsMCP.2222.FFFF filter: " + filter)
		//return "Success", nil
	} else if searchType == WereThereXidFailureMessagesInGkePod {
		genericCore.WriteToLog("searchLogsMCP.2222.GGGG WereThereXidFailureMessagesInGkePod")
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

			genericCore.WriteToLog("searchLogsMCP.2222.HHHH filter: " + filter)
			// Remove later

			//filter += fmt.Sprintf(` AND resource.type="gce_instance" `)
			genericCore.WriteToLog("searchLogsMCP.2222.IIII filter: " + filter)

			if startDateValid {
				//filter = fmt.Sprintf(`resource.type="gce_instance" AND timestamp >= "%s" AND timestamp <= "%s" AND (textPayload:"NVRM: Xid" OR jsonPayload.message:"NVRM: Xid")`, startDate.String(), endDate.String())
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
		genericCore.WriteToLog("searchLogsMCP.2222.KKKK . filter: " + filter)
	}

	genericCore.WriteToLog("searchLogsMCP.3333.AAAA . filter: " + filter)

	// hard coded 128 max results of now
	retMesgStr, returnResults, logSearchSuccess := searchLogsCore(h, ctx, projectID, filter, 128, searchType)

	genericCore.WriteToLog("searchLogsMCP.3333.BBBB . retMesgStr: " + retMesgStr)

	if !logSearchSuccess {
		genericCore.WriteToLog("searchLogsMCP.3333.CCCC")
		return retMesgStr, false, nil
	}

	if searchType == WereThereXidFailureMessagesInGkeCluster {
		if logSearchSuccess {
			// Each array in returnResults has podName, instName, location, xidErr
			retMesgStr = fmt.Sprintf("searchLogsMCP.4444 got %d results for Pods", len(returnResults))
			genericCore.WriteToLog("Success: " + retMesgStr + " but no Xid errors")

			// go through each one of the instances and search their logs

			/*
				url := "https://docs.nvidia.com/deploy/xid-errors/analyzing-xid-catalog.html"
				retTable, xidErrorsScraped, scrapeSuccessful := genericCore.ScrapeURL(url, "Mnemonic")
				if scrapeSuccessful {
					genericCore.WriteToLog("searchLogsMCP.5555")
					thisXidDefinitionArray, foundXidDefinition := genericCore.SearchByColumn1(retTable, returnResults[0][0])
					if foundXidDefinition {
						thisXidDefinitionStr := strings.Join(thisXidDefinitionArray, " ")
						genericCore.WriteToLog("searchLogsMCP.6666 thisXidDefinitionStr" + thisXidDefinitionStr)
						retMesgStr = thisXidDefinitionStr + retMesgStr
					} else {
						genericCore.WriteToLog("searchLogsMCP.7777")
						retMesgStr = xidErrorsScraped + retMesgStr
					}
				}
			*/
			if len(returnResults) != 0 {
				return doInstanceLogsHaveXidErrors(returnResults, h, ctx, projectID), true, nil
			} else {
				return doInstanceLogsHaveXidErrors(returnResults, h, ctx, projectID), false, nil
			}
		}
	} else if searchType == AreNCCLDebugLogsEnabled {
		if logSearchSuccess {
			genericCore.WriteToLog("searchLogsMCP.8888 NCCL Debug is enabled")
			retMesgStr += "NCCL Debug is enabled\n"
		} else {
			genericCore.WriteToLog("searchLogsMCP.9999 NCCL Debug Info NOT found")
			retMesgStr = "NCCL Debug Info NOT found"
		}
	} else if searchType == WereThereNCCLWarnMessages {
		if logSearchSuccess {
			genericCore.WriteToLog("searchLogsMCP.AAAA NCCL WARN messages found")
			retMesgStr += "NCCL WARN messages were found\n"
		} else {
			genericCore.WriteToLog("searchLogsMCP.BBBB NCCL WARN messages NOT found")
			retMesgStr = "NCCL WARN messages NOT found"
		}
	} else if searchType == WereThereNCCLErrorMessages {
		if logSearchSuccess {
			genericCore.WriteToLog("searchLogsMCP.CCCC NCCL ERROR messages found.")
			retMesgStr += "NCCL ERROR messages found.\n"
		} else {
			genericCore.WriteToLog("searchLogsMCP.DDDD NCCL ERROR messages NOT found")
			retMesgStr = "NCCL ERROR messages  NOT found"
		}
	}

	return retMesgStr, false, nil
}

func doInstanceLogsHaveXidErrors(searchResults [][]string, h *handlers, ctx context.Context, projectID string) string {

	filter := ` resource.type="gce_instance"  `

	genericCore.WriteToLog("doInstanceLogsHaveXidErrors.0000")

	for _, arr := range searchResults {
		genericCore.WriteToLog("doInstanceLogsHaveXidErrors.1111 Processing arr: " + arr[0] + " , " + arr[1] + " , " + arr[2] + " , " + arr[3])
		// podName, instName, location, XidErr
		if arr[1] != "" {
			genericCore.WriteToLog("doInstanceLogsHaveXidErrors.2222 inst: " + arr[1])
			filter += fmt.Sprintf(` AND labels."compute.googleapis.com/resource_name"="%s" `, arr[1])
		}
	}

	genericCore.WriteToLog("doInstanceLogsHaveXidErrors.3333")
	resultStr, xidResults, success := searchLogsCore(h, ctx, projectID, filter, 1, WereThereXidFailureMessagesInGceInstance)

	if success {
		t := fmt.Sprintf("Found %v Xid errors in %v GCE instances", len(xidResults), len(searchResults))
		genericCore.WriteToLog("doInstanceLogsHaveXidErrors.4444 : " + t)
		return t
	} else {
		genericCore.WriteToLog("doInstanceLogsHaveXidErrors.5555")
		return resultStr
	}
}

func searchLogsCore(h *handlers, ctx context.Context, projectID string, filter string, maxResults int, searchType LogSearchType) (string, [][]string, bool) {
	genericCore.WriteToLog("-------------------searchLogsCore()-------------------")

	genericCore.WriteToLog("searchLogsCore.0000")
	client, err := logadmin.NewClient(ctx, projectID)
	defer client.Close()
	if err != nil {
		genericCore.WriteToLog("searchLogsCore.1111")
		genericCore.WriteToLog("Could not create logging client")
		return fmt.Sprintf("Could not create logging client: %v", err), nil, false
	}
	genericCore.WriteToLog("searchLogsCore.2222.AAAA")

	genericCore.WriteToLog(fmt.Sprintf("searchLogsCore.2222.BBBB Querying logs with filter: %s\n", filter))
	genericCore.WriteToLog("searchLogsCore.2222.CCCC Scanning for NCCL debug indicators...")

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
			genericCore.WriteToLog("searchLogsCore.3333")
			break
		}
		if err != nil {
			genericCore.WriteToLog("searchLogsCore.4444")
			//searchSuccess = false
			return fmt.Sprintf("Could not iterate over search results: %v", err), nil, false
		}

		genericCore.WriteToLog("searchLogsCore.5555")

		payload = getPayloadString(entry)
		podName = entry.Resource.Labels["pod_name"]
		location = entry.Resource.Labels["location"]

		if searchType == WereThereXidFailureMessagesInGkeCluster {
			genericCore.WriteToLog("searchLogsCore.6666")
			xidErr = parseXidNumber(payload)
			instName = getGceInstanceForPod(podName)
			searchResults = append(searchResults, []string{podName, instName, location, xidErr})
		} else {
			genericCore.WriteToLog("searchLogsCore.7777")
			searchResults = append(searchResults, []string{payload})
		}

		genericCore.WriteToLog("searchLogsCore.8888")
		countResults++
		if countResults > maxResults {
			genericCore.WriteToLog("searchLogsCore.9999")
			break // Found positive confirmation, stop scanning
		}
	}

	genericCore.WriteToLog("searchLogsCore.AAAA")
	return "Found Xid Errors", searchResults, true
}

/*
func getGceInstanceIdFromInstanceName(instName string, projectId string, zone string) (string, bool) {
	ctx := context.Background()
	client, err := compute.NewInstancesRESTClient(ctx)
	defer client.Close()
	if err != nil {
		genericCore.WriteToLog("Could not find instance id for " + instName + fmt.Sprintf("%v", err))
		return "Could not find instance id for " + instName, false
	}

	req := &computepb.GetInstanceRequest{
		Project:  projectId,
		Zone:     zone,
		Instance: instName,
	}

	instance, err := client.Get(ctx, req)
	if err != nil {
		genericCore.WriteToLog("Could not find instance id for " + instName + fmt.Sprintf("%v", err))
		return "Could not find instance id for " + instName, false
	}

	// .GetId() returns the uint64 numeric ID
	return instance.GetId(), true
}
*/

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

	// 2. Get the Node object to find the GCE ProviderID
	//node, err := clientset.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	//if err != nil {
	//	panic(err)
	//}

	genericCore.WriteToLog("getGceInstanceForPod.3333 pod: " + podName + " nodeName: " + nodeName)

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
