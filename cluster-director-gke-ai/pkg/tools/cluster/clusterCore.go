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
	"os/exec"
	"strings"

	"cluster-director-mcp/genericCore"
	//	compute "google.golang.org/api/compute/v0.alpha"
	"google.golang.org/api/compute/v1"
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

var region2ClusterNames = make(map[string][]string)
var clusterNames2JSON = make(map[string]string)
var clusterNames2Zone = make(map[string]string)

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

// Instance holds the parsed data for a single machine.
type Instance struct {
	Name        string
	MachineType string
}

var AllInstancesInProject []Instance
