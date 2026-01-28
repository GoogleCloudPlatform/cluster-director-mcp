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

type SearchLogsRequestXid struct {
	StartDate    string `json:"StartDate,omitempty" jsonschema:"description=Start date to search Cloud Logs"`
	EndDate      string `json:"EndDate,omitempty" jsonschema:"description=End date to search Cloud Logs"`
	NumberOfDays int    `json:"NumberOfDays,omitempty" jsonschema:"default=14,description=Number of days before today to search Cloud Logs"`
	InstanceName string `json:"InstanceName,omitempty" jsonschema:"description=Name of the instance whose logs we should search"`
	JobName      string `json:"JobName,omitempty" jsonschema:"description=Name of the GKE Job whose logs we should search"`
	PodName      string `json:"PodName,omitempty" jsonschema:"description=Name of the pod whose logs we should search"`
}

type SearchLogsResponse struct {
	Status string `json:"status"`
}

type handlers struct {
	c *config.Config
}

type LogSearchType int

const (
	AreNCCLDebugLogsEnabled     LogSearchType = iota // 0
	WereThereNCCLWarnMessages                        // 1
	WereThereNCCLErrorMessages                       // 2
	WereThereXidFailureMessages                      // 3
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
	xidErrorPresentTool := mcp.Tool{
		Name:        "are_xid_present",
		Description: "Search GCP GKE logs for Xid errors",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"StartDate": map[string]interface{}{
					"type": "string",
					//					"format": "%m-%d-%y",
					"format":      "date",
					"description": "The start of the time period to filter search results. Optional argument.",
				},
				"EndDate": map[string]interface{}{
					"type": "string",
					//					"format": "%m-%d-%y",
					"format":      "date",
					"description": "The end of the time period to filter search results. This is an optional argument.",
				},
				"NumberOfDays": map[string]interface{}{
					"type":        "number",
					"description": "Number of days before today to search results. Default value is 14. Optional argument.",
				},
				"InstanceName": map[string]interface{}{
					"type":        "string",
					"description": "Name of the GCE instance whose log to search. Optional argument.",
				},
				"JobName": map[string]interface{}{
					"type":        "string",
					"description": "GKE Job name whose logs to search. Optional argument.",
				},
				"PodName": map[string]interface{}{
					"type":        "string",
					"description": "Name of the GKE pod whose log to search. Optional argument.",
				},
			},
		},
	}
	mcp.AddTool(
		s,
		&xidErrorPresentTool,
		func(ctx context.Context, _ *mcp.CallToolRequest, req SearchLogsRequestXid) (*mcp.CallToolResult, SearchLogsResponse, error) {
			result, err := h.searchLogsMCP(ctx, &req, WereThereXidFailureMessages)
			return nil, SearchLogsResponse{Status: result}, err
		},
	)
}

// func (h *handlers) searchLogsMCP(ctx context.Context, request *SearchLogsRequest, searchType LogSearchType) (string, error) {
func (h *handlers) searchLogsMCP(ctx context.Context, request *SearchLogsRequestXid, searchType LogSearchType) (string, error) {
	genericCore.WriteToLog("searchLogsMCP.0000")
	projectID := h.c.GetDefaultProjectID()
	if projectID == "" {
		return "Could not determine GCP project. Please run: gcloud config set project \"your-project-name\" and restart the AI Assistant", nil
	}

	genericCore.WriteToLog("searchLogsMCP.0000.AAAA")

	clusterName := ""
	var startDate, endDate time.Time
	var startDateStr, endDateStr, instanceName, jobName, podName string
	var numberOfDays int
	var startDateValid, endDateValid bool
	// Xid failures do not input cluster name
	if searchType == WereThereXidFailureMessages {
		genericCore.WriteToLog("searchLogsMCP.1111.AAAA")

		startDateStr = request.StartDate
		startDate, startDateValid = genericCore.ParseTime(startDateStr)

		endDateStr = request.EndDate
		endDate, endDateValid = genericCore.ParseTime(endDateStr)

		numberOfDays = request.NumberOfDays
		if numberOfDays == 0 {
			numberOfDays = 14
		}

		instanceName = request.InstanceName
		jobName = request.JobName
		podName = request.PodName

		if instanceName != "" && podName != "" {
			return "Please specify only one of: InstanceName or PodName", nil
		}
		if instanceName != "" && jobName != "" {
			return "Please specify only one of: InstanceName or JobName", nil
		}
	}
	/*
		else {
			clusterName = request.ClusterName

			genericCore.WriteToLog("searchLogsMCP.1111.BBBB")
			// Since ClusterName is required by the schema, we only check for empty string here
			// for safety, though the SDK should ensure it's present.
			if clusterName == "" {
				genericCore.WriteToLog("searchLogsMCP.1111.CCCC")
				return "Need cluster name", nil
			}
		}
	*/

	genericCore.WriteToLog("searchLogsMCP.2222.AAAA")
	//func searchLogsMCP(projectID string, clusterName string) {
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
	} else if searchType == WereThereXidFailureMessages {
		genericCore.WriteToLog("searchLogsMCP.2222.EEEE WereThereXidFailureMessages")
		filter = fmt.Sprintf(`(textPayload:"NVRM: Xid" OR jsonPayload.message:"NVRM: Xid") `)
		// Force cluster name to an empty string since its not applicable to Xid
		clusterName = ""

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

			// Remove later
			return "Success", nil
		} else {
			filter += fmt.Sprintf(` AND resource.type="gce_instance" `)
		}

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

		genericCore.WriteToLog("searchLogsMCP.2222.FFFF . filter: " + filter)
	}

	if clusterName != "" {
		filter += fmt.Sprintf(` AND resource.labels.cluster_name="%s"`, clusterName)
	}
	genericCore.WriteToLog("searchLogsMCP.3333.AAAA . filter: " + filter)

	retStr, logSearchSuccess := searchLogsCore(h, ctx, projectID, filter)

	genericCore.WriteToLog("searchLogsMCP.3333.BBBB . retStr: " + retStr)

	if !logSearchSuccess {
		return retStr, nil
	}

	if searchType == WereThereXidFailureMessages {
		if retStr != "" {
			xidStr := parseXidNumber(retStr)

			genericCore.WriteToLog("searchLogsMCP.4444 . xidStr" + xidStr)

			url := "https://docs.nvidia.com/deploy/xid-errors/analyzing-xid-catalog.html"
			retTable, xidErrorsScraped, scrapeSuccessful := genericCore.ScrapeURL(url, "Mnemonic")

			if scrapeSuccessful {
				genericCore.WriteToLog("searchLogsMCP.5555")
				thisXidDefinitionArray, foundXidDefinition := genericCore.SearchByColumn1(retTable, xidStr)
				if foundXidDefinition {
					thisXidDefinitionStr := strings.Join(thisXidDefinitionArray, " ")
					genericCore.WriteToLog("searchLogsMCP.6666 thisXidDefinitionStr" + thisXidDefinitionStr)
					retStr = thisXidDefinitionStr + retStr
				} else {
					genericCore.WriteToLog("searchLogsMCP.7777")
					retStr = xidErrorsScraped + retStr
				}
			}
		}
	} else if searchType == AreNCCLDebugLogsEnabled {
		if retStr != "" {
			genericCore.WriteToLog("searchLogsMCP.8888 NCCL Debug is enabled")
			retStr += "NCCL Debug is enabled\n"
		} else {
			genericCore.WriteToLog("searchLogsMCP.9999 NCCL Debug Info NOT found")
			retStr = "NCCL Debug Info NOT found"
		}
	} else if searchType == WereThereNCCLWarnMessages {
		if retStr != "" {
			genericCore.WriteToLog("searchLogsMCP.AAAA NCCL WARN messages found")
			retStr += "NCCL WARN messages were found\n"
		} else {
			genericCore.WriteToLog("searchLogsMCP.BBBB NCCL WARN messages NOT found")
			retStr = "NCCL WARN messages NOT found"
		}
	} else if searchType == WereThereNCCLErrorMessages {
		if retStr != "" {
			genericCore.WriteToLog("searchLogsMCP.CCCC NCCL ERROR messages found.")
			retStr += "NCCL ERROR messages found.\n"
		} else {
			genericCore.WriteToLog("searchLogsMCP.DDDD NCCL ERROR messages NOT found")
			retStr = "NCCL ERROR messages  NOT found"
		}
	}

	return retStr, nil
}

func searchLogsCore(h *handlers, ctx context.Context, projectID string, filter string) (string, bool) {
	genericCore.WriteToLog("-------------------searchLogsCore()-------------------")

	genericCore.WriteToLog("searchLogsCore.0000")
	client, err := logadmin.NewClient(ctx, projectID)
	defer client.Close()
	if err != nil {
		genericCore.WriteToLog("searchLogsCore.1111")
		genericCore.WriteToLog("Could not create logging client")
		return fmt.Sprintf("Could not create logging client: %v", err), false
	}
	genericCore.WriteToLog("searchLogsCore.2222")

	genericCore.WriteToLog(fmt.Sprintf("Querying logs with filter: %s\n", filter))
	genericCore.WriteToLog("Scanning for NCCL debug indicators...")

	it := client.Entries(ctx, logadmin.Filter(filter))

	// This tells the GCP Server "only send me 1 item per page"
	it.PageInfo().MaxSize = 1

	var sampleLog = ""
	//searchSuccess := true
	// Iterate through the logs
	for {
		entry, err := it.Next()
		if err == iterator.Done {
			genericCore.WriteToLog("searchLogsCore.3333")
			return "Search Results were empty", false
		}
		if err != nil {
			genericCore.WriteToLog("searchLogsCore.4444")
			//searchSuccess = false
			return fmt.Sprintf("Could not iterate over search results: %v", err), false
		}

		genericCore.WriteToLog("searchLogsCore.5555")
		payload := getPayloadString(entry)
		sampleLog = payload
		break // Found positive confirmation, stop scanning
	}

	genericCore.WriteToLog("searchLogsCore.6666")

	genericCore.WriteToLog("searchLogsCore.7777")
	return sampleLog, true
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

func getGceInstanceForPod(podName string) (string, bool) {
	// hard coded fix later
	namespace := "default"

	// Setup Kubernetes client
	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	config, _ := clientcmd.BuildConfigFromFlags("", kubeconfig)
	clientset, _ := kubernetes.NewForConfig(config)

	// 1. Get the Pod object
	pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		return "Could not get node name for pod " + podName, false
	}

	nodeName := pod.Spec.NodeName
	fmt.Printf("Pod %s is running on Node: %s\n", podName, nodeName)

	// 2. Get the Node object to find the GCE ProviderID
	//node, err := clientset.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	//if err != nil {
	//	panic(err)
	//}

	// The ProviderID format is: gce://project-id/zone/instance-name
	//fmt.Printf("GCE Provider ID: %s\n", node.Spec.ProviderID)
	return nodeName, true
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
