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
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	compute "google.golang.org/api/compute/v0.alpha"

	"github.com/GoogleCloudPlatform/cluster-director-mcp/pkg/genericCore"
	"github.com/GoogleCloudPlatform/cluster-director-mcp/pkg/persistence"
)

var authToken string

// One-2-Many Mapping, i.e one region maps to a one or more zones
var regions2Zones = make(map[string][]string)

// The root struct that holds information on a list of clusters.
type ClustersResponse struct {
	Clusters []Cluster `json:"clusters"`
}

// Key to this map is the clusters region
var MostRecentClusterData = make(map[string]*ClustersResponse)

var (
	region2ClusterNames = make(map[string][]string)
	clusterNames2JSON   = make(map[string]string)
	clusterNames2Zone   = make(map[string]string)
)

// Cluster defines the top-level structure of the JSON object returned
// from Cluster Director API
type Cluster struct {
	Name         string       `json:"name"`
	CreateTime   string       `json:"createTime"`
	UpdateTime   string       `json:"updateTime"`
	Networks     []Network    `json:"networks"`
	Storages     []Storage    `json:"storages"`
	Compute      Compute      `json:"compute"`
	Orchestrator Orchestrator `json:"orchestrator"`
	Reconciling  bool         `json:"reconciling"`
}

// Network corresponds to an object in the "networks" array.
type Network struct {
	Network          string `json:"network"`
	InitializeParams struct {
		Network string `json:"network"`
	} `json:"initializeParams"`
	Subnetwork string `json:"subnetwork"`
}

// Storage corresponds to an object in the "storages" array.
type Storage struct {
	Storage          string `json:"storage"`
	InitializeParams struct {
		Filestore struct {
			FileShares []struct {
				CapacityGb string `json:"capacityGb"`
				FileShare  string `json:"fileShare"`
			} `json:"fileShares"`
			Tier      string `json:"tier"`
			Filestore string `json:"filestore"`
			Protocol  string `json:"protocol"`
		} `json:"filestore"`
	} `json:"initializeParams"`
	ID string `json:"id"`
}

// Compute corresponds to the "compute" object.
type Compute struct {
	ResourceRequests []ResourceRequest `json:"resourceRequests"`
}

// ResourceRequest corresponds to an object in the "resourceRequests" array.
type ResourceRequest struct {
	ID                string                   `json:"id"`
	Zone              string                   `json:"zone"`
	MachineType       string                   `json:"machineType"`
	GuestAccelerators []map[string]interface{} `json:"guestAccelerators"`
	Disks             []Disk                   `json:"disks"`
	ProvisioningModel string                   `json:"provisioningModel"`
}

// Disk corresponds to a disk object.
type Disk struct {
	Type        string `json:"type"`
	SizeGb      string `json:"sizeGb"`
	Boot        bool   `json:"boot"`
	SourceImage string `json:"sourceImage"`
}

// Orchestrator corresponds to the "orchestrator" object.
type Orchestrator struct {
	Slurm Slurm `json:"slurm"`
}

// Slurm corresponds to the "slurm" object.
type Slurm struct {
	NodeSets         []NodeSet   `json:"nodeSets"`
	Partitions       []Partition `json:"partitions"`
	DefaultPartition string      `json:"defaultPartition"`
	LoginNodes       LoginNodes  `json:"loginNodes"`
}

// NodeSet corresponds to an object in the "nodeSets" array.
type NodeSet struct {
	ID                string          `json:"id"`
	ResourceRequestID string          `json:"resourceRequestId"`
	StorageConfigs    []StorageConfig `json:"storageConfigs"`
	StaticNodeCount   string          `json:"staticNodeCount"`
	EnableOsLogin     bool            `json:"enableOsLogin"`
}

// Partition corresponds to an object in the "partitions" array.
type Partition struct {
	ID         string   `json:"id"`
	NodeSetIDs []string `json:"nodeSetIds"`
}

// LoginNodes corresponds to the "loginNodes" object.
type LoginNodes struct {
	MachineType     string `json:"machineType"`
	Zone            string `json:"zone"`
	Count           string `json:"count"`
	Disks           []Disk `json:"disks"`
	EnableOsLogin   bool   `json:"enableOsLogin"`
	EnablePublicIps bool   `json:"enablePublicIps"`
	Instances       []struct {
		Instance string `json:"instance"`
	} `json:"instances"`
	StorageConfigs []StorageConfig `json:"storageConfigs"`
}

// StorageConfig corresponds to a storage configuration object.
type StorageConfig struct {
	ID         string `json:"id"`
	LocalMount string `json:"localMount"`
}

// getGCloudToken executes the 'gcloud auth print-access-token' command
// and caches the OAuth token.
func getGCloudToken() bool {
	if authToken != "" {
		return true
	}

	genericCore.WriteToLog("Executing 'gcloud auth print-access-token' to get bearer token...")

	// Prepare the command
	cmd := exec.Command("gcloud", "auth", "print-access-token")

	// Run the command and capture its output
	output, err := cmd.Output()
	if err != nil {
		// If 'gcloud' is not installed or not in the PATH, this will fail.
		// It can also fail if the user is not authenticated.
		genericCore.WriteToLog(fmt.Sprintf("Error running gcloud command: %v", err))
		return false
	}

	// The output is a byte slice, so we convert it to a string and
	// trim any trailing newline or whitespace.
	authToken = strings.TrimSpace(string(output))
	genericCore.WriteToLog("Successfully retrieved access token.")
	return true
}

func getAllZonesInRegion(region string, projectID string, ctx context.Context, computeService *compute.Service) []string {
	var zonesList []string

	// The filter string tells the API to return only zones whose region name
	// matches the one we specified.
	filter := fmt.Sprintf("name=%s-*", region)

	// Call the Zones.List method with the project ID and the filter.
	req1 := computeService.Zones.List(projectID).Filter(filter)

	// The 'Do' method handles pagination for you. We process each page of results.
	if err := req1.Pages(ctx, func(page *compute.ZoneList) error {
		for _, zone := range page.Items {
			genericCore.WriteToLog(zone.Name)
			zonesList = append(zonesList, zone.Name)
		}
		return nil
	}); err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error getting zones for project %s in region %s : %v",
			projectID, region, err))
	}
	return zonesList
}

func getAllRegionsAndZonesSupportedByHCS(projectID string) bool {
	// Location represents a single location object inside the array.
	type Location struct {
		Name       string `json:"name"`
		LocationID string `json:"locationId"`
	}

	// LocationList represents the top-level JSON object.
	type LocationList struct {
		Locations []Location `json:"locations"`
	}

	// Declare a variable of your top-level struct type
	var locationData LocationList

	// Equivalent CURL command:
	// curl \
	// -H "Content-Type:application/json" \
	// -H "Authorization: Bearer $(gcloud auth print-access-token)" \
	// https://hypercomputecluster.googleapis.com/v1alpha/projects/cloud-hypercomp-dev/locations/us-central1/clusters
	url := "https://hypercomputecluster.googleapis.com/v1alpha/projects/" + projectID + "/locations/"

	bodyJson, success := genericCore.QueryURLAndGetResult(authToken, url)
	if !success {
		genericCore.WriteToLog("Error getting list of zones supported by Cluster Director")
		return false
	}

	// Unmarshal the JSON data into the locationData variable
	// We pass the JSON string as a byte slice and a pointer to our variable.
	err := json.Unmarshal([]byte(bodyJson), &locationData)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error unmarshaling JSON: %v", err))
		return false
	}

	ctx := context.Background()
	computeService, err := compute.NewService(ctx)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error calling compute.NewSerice() API: %v", err))
		return false
	}
	// Now you can access the data through the struct
	for _, loc := range locationData.Locations {
		genericCore.WriteToLog("Region: " + loc.LocationID)
		regions2Zones[loc.LocationID] = getAllZonesInRegion(loc.LocationID, projectID, ctx, computeService)
	}

	return true
}

// Return zone for a cluster if it is present in the cached data structure
func getZoneForCluster(projectID string, clusterName string) string {
	var zone string

	zone, exists := clusterNames2Zone[clusterName]
	if exists {
		return zone
	} else {
		getClustersInAllRegions(projectID)
		zone, exists = clusterNames2Zone[clusterName]
		if exists {
			return zone
		} else {
			genericCore.WriteToLog("Error getting zone for cluster " + clusterName + " in project " + projectID)
			return ""
		}
	}
}

func getClustersInAllRegions(projectID string) string {
	var listOfClusters string = "["
	genericCore.WriteToLog(fmt.Sprintf("Getting clusters in all regions for projectId : %s", projectID))
	for region := range regions2Zones {
		genericCore.WriteToLog(fmt.Sprintf("Getting clusters in region : %s", region))
		getClustersInRegionIfExists(region, projectID)
		for _, clusterName := range region2ClusterNames[region] {
			genericCore.WriteToLog(fmt.Sprintf("Found cluster : %s", clusterName))
			listOfClusters += string("\"" + clusterName + "\", ")
		}
	}
	listOfClusters = strings.TrimSuffix(listOfClusters, ", ")
	listOfClusters += "]"
	genericCore.WriteToLog(fmt.Sprintf("Final list of clusters in all regions in project %s : %s ", projectID, listOfClusters))

	return listOfClusters
}

func getClustersInRegionIfExists(region string, projectID string) {
	// Equivalent CURL command:
	// curl \
	// -H "Content-Type:application/json" \
	// -H "Authorization: Bearer $(gcloud auth print-access-token)" \
	// https://hypercomputecluster.googleapis.com/v1alpha/projects/cloud-hypercomp-dev/locations/us-central1/clusters
	url := "https://hypercomputecluster.googleapis.com/v1alpha/projects/" + projectID + "/locations/" + region + "/clusters"
	genericCore.WriteToLog(fmt.Sprintf("getClustersInRegionIfExists - Getting clusters in region %s URL : %s", region, url))

	// Remove all previous data about clusters in this region
	region2ClusterNames[region] = []string{}

	bodyString, success := genericCore.QueryURLAndGetResult(authToken, url)
	genericCore.WriteToLog(fmt.Sprintf("Response from Cluster Director API on the clusters in region %s : %s ", region, string(bodyString)))
	// If the body has "storages" than that means it is a cluster
	if success && strings.Contains(bodyString, "storages") {
		genericCore.WriteToLog("Trying to parse JSON...")
		var parsedClusterData ClustersResponse
		err := json.Unmarshal([]byte(bodyString), &parsedClusterData)

		// Force a deep copy by assigning pointer
		MostRecentClusterData[region] = &parsedClusterData

		if err != nil {
			genericCore.WriteToLog(fmt.Sprintf("Error unmarshalling JSON: %v", err))
		} else {
			genericCore.WriteToLog(fmt.Sprintf("Number of elements in parsedClusterData.Clusters: %d", len(MostRecentClusterData[region].Clusters)))
			for i := range MostRecentClusterData[region].Clusters {
				clusterName := filepath.Base(MostRecentClusterData[region].Clusters[i].Name)
				genericCore.WriteToLog(fmt.Sprintf("Processing JSON data for cluster : %s", clusterName))
				for j := range MostRecentClusterData[region].Clusters[i].Compute.ResourceRequests {
					clusterZone := MostRecentClusterData[region].Clusters[i].Compute.ResourceRequests[j].Zone
					genericCore.WriteToLog(fmt.Sprintf("Zone for cluster : %s", clusterZone))
					clusterNames2Zone[clusterName] = clusterZone
				}
				region2ClusterNames[region] = append(region2ClusterNames[region], clusterName)
				clusterNames2JSON[clusterName] = string(bodyString)
			}
		}
	} else {
		genericCore.WriteToLog("The response body does not contain the substring 'storages'.")
	}
}

// Instance holds the parsed data for a single machine.
type Instance struct {
	Name        string
	MachineType string
}

var AllInstancesInProject []Instance

// PartitionInfo holds the structured data for a single line from the slurm command "sinfo"
// sample output of "sinfo"
// PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
// part1*       up   infinite      1  idle# xxxxx-nodeset1-0
// part1*       up   infinite      3   idle xxxxx-nodeset1-[1-3]
type PartitionInfo struct {
	Name      string
	IsDefault bool
	Avail     string
	TimeLimit string
	Nodes     int
	State     string
	NodeList  string
}

func parseOutputofSlurmSinfoCmdAndReturnPartitions(output string) (map[string][]string, map[string]struct{}, bool) {
	genericCore.WriteToLog("Raw output from sinfo : " + output)

	partitions := make(map[string][]string)
	clusterStates := make(map[string]struct{})

	// Split the output into lines and trim any surrounding whitespace.
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Make sure we have more than just the header line.
	if len(lines) < 2 {
		return nil, nil, false
	}

	// Iterate over the lines, skipping the header at index 0.
	for _, line := range lines[1:] {
		genericCore.WriteToLog("Parsing sinfo line: " + line)
		if strings.Contains(line, "PARTITION") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 6 {
			// Skip any malformed or empty lines.
			continue
		}

		// trim any chars that is not alphabet or letter
		partitionName := strings.TrimRightFunc(fields[0], func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) })
		genericCore.WriteToLog("Parsed partition name : " + partitionName)

		partitions[partitionName] = append(partitions[partitionName], fields[5])

		// state is something like idle#, idle~ down, alloc ...etc
		clusterStates[fields[4]] = struct{}{}
	}

	return partitions, clusterStates, true
}

func GetDetailedJobInfoForAllRunningCDMcpJobsOfUserInCluster(projectId string,
	clusterName string,
	zone string,
	loginNode string,
) (map[int]int, bool, string) {
	// Key is cluster-director-mc Job Id, value is slurm Job Id
	jobDataMap := make(map[int]int)

	jobArray, errorMsg, success := GetRunningSlurmJobsForUserInCluster(projectId, clusterName, zone, loginNode)
	if !success {
		return jobDataMap, false, errorMsg
	}

	r := regexp.MustCompile(`CDMcpJobId\.(\d+)`)
	for _, slurmJobId := range jobArray {
		scontrolCmd := fmt.Sprintf("scontrol show job -dd %d", slurmJobId)
		sshOut, success := runSSHOnNode(loginNode, projectId, zone, scontrolCmd)
		if !success {
			s := string(fmt.Sprintf("Could not run scontrol to get info for job %d", slurmJobId) +
				" on login node " + loginNode + " in cluster " + clusterName + " in project " + projectId + "\n" + genericCore.GetLastLines(sshOut, 10))
			genericCore.WriteToLog(s)
			return jobDataMap, false, s
		}

		if strings.Contains(sshOut, "/"+strings.ReplaceAll(persistence.CDMCP_REMOTE_ROOT_DIR, "~/", "")) {
			matches := r.FindStringSubmatch(sshOut)
			// 3. Check if we found a match (and our capture group)
			if len(matches) < 2 {
				continue
			}

			// 4. The captured number is in matches[1] (as a string)
			numStr := matches[1]

			// 5. Convert the string to an integer
			CDMcpJobId, err := strconv.Atoi(numStr)
			if err != nil {
				genericCore.WriteToLog(fmt.Sprintf("Error converting matched string to integer:" + err.Error()))
				continue
			}
			genericCore.WriteToLog(fmt.Sprintf("Cluster Director MCP JobId CDMcpJobId : %d", CDMcpJobId))
			jobDataMap[CDMcpJobId] = slurmJobId
		} else {
			genericCore.WriteToLog("")
		}
	}

	genericCore.WriteToLog(fmt.Sprintf("Returning %d jobs \n", len(jobDataMap)))
	return jobDataMap, true, ""
}

func GetRunningSlurmJobsForUserInCluster(projectId string, clusterName string, zone string, loginNode string) ([]int, string, bool) {
	var runningJobIds []int

	currentUser, err := user.Current()
	if err != nil {
		return runningJobIds, "Could not get user-id (login name/LDAP)", false
	}

	sqCmd := "squeue -u $USER -t  RUNNING "
	sshOut, success := runSSHOnNode(loginNode, projectId, zone, sqCmd)
	if !success {
		genericCore.WriteToLog("GetRunningSlurmJobsForUserInCluster.3333")
		return runningJobIds, "Could not run squeue to get running jobs on " +
			loginNode + " in cluster " + clusterName + " in project " + projectId + "\n" + genericCore.GetLastLines(sshOut, 10), false
	}

	// Sample Output of : squeue -u $USER -t RUNNING
	// JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
	// 147     part1 build-nc ext_xxxx  R       1:47      1 xxxx-nodeset1-0
	if err != nil {
		return runningJobIds, "Could not get running jobs for user " + currentUser.Username + " using: squeue -u " + currentUser.Username + " -t RUNNING", false
	}

	isNumericRegex := regexp.MustCompile(`^\d+$`)
	scanner := bufio.NewScanner(strings.NewReader(sshOut))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)

		// 8 fields
		if len(fields) != 8 {
			continue // malformed due to wrong number of tokens
		}
		if isNumericRegex.MatchString(fields[0]) {
			jobId, err := strconv.Atoi(fields[0])
			if err != nil {
				genericCore.WriteToLog("Error converting " + fields[0] + " to integer")
				continue
			}
			runningJobIds = append(runningJobIds, jobId)
		}
	}

	genericCore.WriteToLog(fmt.Sprintf("Number of running jobs: %d", len(runningJobIds)))
	return runningJobIds, "", true
}

func GetMachineTypeForCluster(projectId string, clusterName string) []string {
	getClustersInAllRegions(projectId)

	var machineTypes []string

	genericCore.WriteToLog("Trying to determine machine for cluster " + clusterName)
	for region := range MostRecentClusterData {
		genericCore.WriteToLog("Looking for cluster info in region region : " + region)
		for i := range MostRecentClusterData[region].Clusters {
			if clusterName != filepath.Base(MostRecentClusterData[region].Clusters[i].Name) {
				genericCore.WriteToLog("Ignoring because of name mismatch " + MostRecentClusterData[region].Clusters[i].Name)
				continue
			}
			genericCore.WriteToLog("Found cluster " + MostRecentClusterData[region].Clusters[i].Name)
			for j := range MostRecentClusterData[region].Clusters[i].Compute.ResourceRequests {
				if MostRecentClusterData[region].Clusters[i].Compute.ResourceRequests[j].MachineType != "" {
					machineTypes = append(machineTypes, MostRecentClusterData[region].Clusters[i].Compute.ResourceRequests[j].MachineType)
				}
			}
		}
	}

	genericCore.WriteToLog(fmt.Sprintf("Returning %d machineTypes", len(machineTypes)))
	return machineTypes
}
