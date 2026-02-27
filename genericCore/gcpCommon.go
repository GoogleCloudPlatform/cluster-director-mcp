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

package genericCore

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

var authToken string

// GcloudListItem represents a single item from the gcloud list command's JSON output.
type GcloudListItem struct {
	Name string `json:"name"`
}

// CheckConsumptionRequestShared defines the input from the tool
type CheckConsumptionRequestShared struct {
	InstanceName string
	Zone         string
	ProjectID    string
}

// InstanceConsumptionStatus defines the JSON output structure
type InstanceConsumptionStatus struct {
	InstanceName        string `json:"instance_name"`
	Zone                string `json:"zone"`
	ProvisioningModel   string `json:"provisioning_model"`
	ReservationAffinity string `json:"reservation_affinity"`
	ConsumptionStatus   string `json:"consumption_status"`
}

// LogProcessor is a callback function passed by the caller.
// It returns the formatted slice of strings to store, and a boolean indicating if it should be included.
type LogProcessor func(entry *logging.Entry) ([]string, bool)

// GetGCloudToken executes the 'gcloud auth print-access-token' command
// and caches the OAuth token.
func GetGCloudToken() bool {
	if authToken != "" {
		return true
	}

	WriteToLog("Fetching OAuth2 bearer token natively via ADC...")

	// Use the default context and cloud-platform scope
	ctx := context.Background()
	scopes := []string{"https://www.googleapis.com/auth/cloud-platform"}

	ts, err := google.DefaultTokenSource(ctx, scopes...)
	if err != nil {
		WriteToLog(fmt.Sprintf("Failed to find Default Token Source: %v", err))
		return false
	}

	token, err := ts.Token()
	if err != nil {
		WriteToLog(fmt.Sprintf("Error retrieving native token: %v", err))
		return false
	}

	authToken = token.AccessToken
	WriteToLog("Successfully retrieved access token natively.")
	return true
}

// GetCachedAuthToken returns the currently cached OAuth token.
func GetCachedAuthToken() string {
	return authToken
}

func RunSSHOnNode(hostName string, project string, zone string, cmd string) (string, bool) {
	sshCmd := exec.Command("/usr/bin/gcloud",
		"compute",
		"ssh",
		hostName,
		"--project="+project,
		"--zone="+zone,
		"--tunnel-through-iap",
		"--command",
		cmd)

	output, err := sshCmd.CombinedOutput()
	rawSSHOutput := strings.TrimSpace(string(output))
	filteredSSHOutput := FilterSSHOutput(rawSSHOutput)
	WriteToLog(string(filteredSSHOutput))
	if err != nil {
		WriteToLog(fmt.Sprintf("Error running SSH cmd: %s %v", cmd, err))
		return filteredSSHOutput, false
	}

	return filteredSSHOutput, true
}

func RunSCP(project string, zone string, srcFile string, destFile string) (string, bool) {
	finalSCPCmd := exec.Command("/usr/bin/gcloud",
		"compute",
		"scp",
		"--project="+project,
		"--zone="+zone,
		"--tunnel-through-iap",
		srcFile,
		destFile)

	output, err := finalSCPCmd.CombinedOutput()
	scpOutput := strings.TrimSpace(string(output))
	filteredSCPOutput := FilterSSHOutput(scpOutput)
	WriteToLog(string(filteredSCPOutput))
	if err != nil {
		WriteToLog(fmt.Sprintf("Error running SCP: %v", err))
		return filteredSCPOutput, false
	}

	return filteredSCPOutput, true
}

// SearchLogsCore executes a log search using the GCP SDK.
// It delegates all parsing and filtering logic to the provided LogProcessor callback.
func SearchLogsCore(ctx context.Context, projectID string, filter string, maxResults int, processor LogProcessor) (string, [][]string, bool) {
	WriteToLog("-------------------SearchLogsCore()-------------------")

	client, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		WriteToLog("Could not create logging client")
		return fmt.Sprintf("Could not create logging client: %v", err), nil, false
	}
	defer client.Close()

	it := client.Entries(ctx, logadmin.Filter(filter))

	countResults := 0
	var searchResults [][]string

	for {
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Sprintf("Could not iterate over search results: %v", err), nil, false
		}

		// Let the callback function decide how to parse the entry, and if we should include it
		row, include := processor(entry)
		if include {
			searchResults = append(searchResults, row)
			countResults++
		}

		if countResults >= maxResults {
			break
		}
	}

	return "Search completed", searchResults, len(searchResults) > 0
}

// GetPayloadString extracts string content from either text or JSON payloads.
// Exported so the caller's callback function can use it.
func GetPayloadString(entry *logging.Entry) string {
	switch p := entry.Payload.(type) {
	case string:
		return p
	case *structpb.Struct:
		if val, ok := p.Fields["message"]; ok {
			return val.GetStringValue()
		}
		if val, ok := p.Fields["log"]; ok {
			return val.GetStringValue()
		}
		return p.String()
	default:
		return fmt.Sprintf("%v", p)
	}
}

// RunGcloudListCommand executes a 'gcloud compute <resource> list' command and returns the names.
func RunGcloudListCommand(ctx context.Context, projectID string, resource string) ([]string, error) {
	// Initialize the native Compute Service
	// Passing nil for options ensures it uses ADC
	service, err := compute.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create native compute service: %w", err)
	}

	var names []string

	switch resource {
	case "regions":
		req := service.Regions.List(projectID)
		if err := req.Pages(ctx, func(page *compute.RegionList) error {
			for _, r := range page.Items {
				names = append(names, r.Name)
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("native regions list failed: %w", err)
		}

	case "zones":
		req := service.Zones.List(projectID)
		if err := req.Pages(ctx, func(page *compute.ZoneList) error {
			for _, z := range page.Items {
				names = append(names, z.Name)
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("native zones list failed: %w", err)
		}

	default:
		return nil, fmt.Errorf("resource type %s not yet implemented in native SDK", resource)
	}

	WriteToLog(fmt.Sprintf("Native API successfully retrieved %d %s", len(names), resource))
	return names, nil
}

// GetGCloudRegionsAndZones fetches all available GCP regions and zones using the gcloud CLI.
func GetGCloudRegionsAndZones(ctx context.Context, projectID string) ([]string, []string, error) {
	regions, err := RunGcloudListCommand(ctx, projectID, "regions")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get regions: %w", err)
	}

	zones, err := RunGcloudListCommand(ctx, projectID, "zones")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get zones: %w", err)
	}

	return regions, zones, nil
}

// GetResourceNameFromURL extracts the last part of a GCP URL (e.g., "us-central1-a" from ".../zones/us-central1-a")
func GetResourceNameFromURL(url string) string {
	parts := strings.Split(url, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return url
}

// This mimics the "list_clusters" behavior: if we don't know where it is, we search everywhere.
func FindInstanceInProject(ctx context.Context, service *compute.Service, projectID string, instanceName string) (*compute.Instance, string, error) {
	filter := fmt.Sprintf("name = \"%s\"", instanceName)

	var foundInstance *compute.Instance
	var foundZone string

	err := service.Instances.AggregatedList(projectID).Filter(filter).Pages(ctx, func(page *compute.InstanceAggregatedList) error {
		for _, scopedList := range page.Items {
			if len(scopedList.Instances) > 0 {
				for _, inst := range scopedList.Instances {
					if inst.Name == instanceName {
						foundInstance = inst
						foundZone = GetResourceNameFromURL(inst.Zone)
						return nil
					}
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, "", fmt.Errorf("failed to search project %s: %v", projectID, err)
	}

	if foundInstance == nil {
		return nil, "", fmt.Errorf("instance '%s' not found in any zone of project '%s'", instanceName, projectID)
	}

	return foundInstance, foundZone, nil
}

// CheckInstanceConsumptionCore is the main logic function.
// It handles Auto-Discovery, Spot Checks, and Reservation Checks.
func CheckInstanceConsumptionCore(ctx context.Context, req CheckConsumptionRequestShared, defaultProjectID string) (InstanceConsumptionStatus, error) {
	WriteToLog("CheckInstanceConsumptionCore.0000")

	req.InstanceName = strings.TrimSpace(req.InstanceName)
	req.Zone = strings.TrimSpace(req.Zone)
	req.ProjectID = strings.TrimSpace(req.ProjectID)

	WriteToLog(fmt.Sprintf("Evaluating consumption status for instance: '%s'", req.InstanceName))

	var status InstanceConsumptionStatus
	status.InstanceName = req.InstanceName
	status.Zone = req.Zone

	projectID := req.ProjectID
	if projectID == "" {
		projectID = defaultProjectID
	}

	service, err := compute.NewService(ctx, option.WithScopes(compute.ComputeScope))
	if err != nil {
		status.ConsumptionStatus = fmt.Sprintf("Failed to create compute service: %v", err)
		return status, nil
	}

	var instance *compute.Instance

	if req.Zone != "" {
		inst, err := service.Instances.Get(projectID, req.Zone, req.InstanceName).Context(ctx).Do()
		if err == nil {
			instance = inst
			status.Zone = req.Zone
		} else {
			WriteToLog(fmt.Sprintf("Direct fetch failed for %s in %s: %v. Falling back to global search.", req.InstanceName, req.Zone, err))
		}
	}

	if instance == nil {
		foundInst, foundZone, err := FindInstanceInProject(ctx, service, projectID, req.InstanceName)
		if err != nil {
			status.ConsumptionStatus = fmt.Sprintf("Error: %v. Check your PROJECT_ID configuration.", err)
			return status, nil
		}
		instance = foundInst
		status.Zone = foundZone
	}

	//  Check Spot / Preemptible Status
	isSpot := false
	status.ProvisioningModel = "STANDARD VM"
	if instance.Scheduling != nil {
		if instance.Scheduling.ProvisioningModel == "SPOT" {
			status.ProvisioningModel = "SPOT VM (No max duration)"
			isSpot = true
		} else if instance.Scheduling.Preemptible {
			status.ProvisioningModel = "LEGACY PREEMPTIBLE VM (24h max duration)"
			isSpot = true
		}
	}

	//  Check Reservation Status
	if isSpot {
		status.ReservationAffinity = "None (Spot VM)"
		status.ConsumptionStatus = "Not consuming (Spot VMs cannot use reservations)"
	} else {
		consumeType := "ANY_RESERVATION"
		if instance.ReservationAffinity != nil {
			consumeType = instance.ReservationAffinity.ConsumeReservationType
		}

		switch consumeType {
		case "NO_RESERVATION":
			status.ReservationAffinity = "None (Explicitly disabled)"
			status.ConsumptionStatus = "Not consuming (On-Demand)"

		case "SPECIFIC_RESERVATION":
			key := ""
			val := ""
			if instance.ReservationAffinity != nil {
				key = instance.ReservationAffinity.Key
				if len(instance.ReservationAffinity.Values) > 0 {
					val = instance.ReservationAffinity.Values[0]
				}
			}
			status.ReservationAffinity = fmt.Sprintf("Specific (Target: %s=%s)", key, val)
			status.ConsumptionStatus = "Consuming (Specific Reservation)"

		case "ANY_RESERVATION":
			status.ReservationAffinity = "Automatic (Any matching reservation)"
			foundMatchName := ""
			reqRes := service.Reservations.List(projectID, status.Zone)

			_ = reqRes.Pages(ctx, func(page *compute.ReservationList) error {
				for _, res := range page.Items {
					if res.SpecificReservationRequired || res.Status != "READY" {
						continue
					}
					if res.SpecificReservation != nil && res.SpecificReservation.InstanceProperties != nil {
						resMachineType := GetResourceNameFromURL(res.SpecificReservation.InstanceProperties.MachineType)
						instMachineType := GetResourceNameFromURL(instance.MachineType)

						if resMachineType == instMachineType {
							foundMatchName = res.Name
							return nil
						}
					}
				}
				return nil
			})

			if foundMatchName != "" {
				status.ConsumptionStatus = fmt.Sprintf("Consuming (%s)", foundMatchName)
			} else {
				status.ConsumptionStatus = "Not consuming (No matching reservation found)"
			}

		default:
			status.ReservationAffinity = "Unknown"
			status.ConsumptionStatus = "Not consuming (On-Demand)"
		}
	}

	return status, nil
}

// ProcessConsumptionRequest handles the loop, error catching, and JSON formatting.
func ProcessConsumptionRequest(ctx context.Context, instanceNames []string, zone string, reqProjectID string, defaultProjectID string) (string, error) {
	var results []InstanceConsumptionStatus

	for _, name := range instanceNames {
		sharedReq := CheckConsumptionRequestShared{
			InstanceName: name,
			Zone:         zone,
			ProjectID:    reqProjectID,
		}

		info, err := CheckInstanceConsumptionCore(ctx, sharedReq, defaultProjectID)

		if err != nil {
			results = append(results, InstanceConsumptionStatus{
				InstanceName:      name,
				ConsumptionStatus: fmt.Sprintf("Error: %v", err),
			})
		} else {
			results = append(results, info)
		}
	}

	if len(results) == 0 {
		return "[]", nil
	}

	jsonBytes, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to generate JSON output: %v", err)
	}

	return string(jsonBytes), nil
}

// CheckStockoutErrorsCore executes a log search query for ZONE_RESOURCE_POOL_EXHAUSTED
// over a given timeframe. It extracts the affected instance names and timestamps, cross-references
// reservations, and checks the consumption type of at least one instance.
func CheckStockoutErrorsCore(ctx context.Context, defaultProjectID string, reqProjectID string, startDateStr string, endDateStr string, numberOfDays int, clusterFilter string, clusterName string) (string, error) {
	WriteToLog("CheckStockoutErrorsCore.0000")

	projectID := reqProjectID
	if projectID == "" {
		projectID = defaultProjectID
	}

	if numberOfDays <= 0 {
		numberOfDays = 14
	}

	start := time.Now().AddDate(0, 0, -numberOfDays)
	end := time.Now()

	if startDateStr != "" {
		if parsedStart, ok := ParseTime(startDateStr); ok {
			start = parsedStart
		}
	}
	if endDateStr != "" {
		if parsedEnd, ok := ParseTime(endDateStr); ok {
			end = parsedEnd
		}
	}

	// Filter specifically for GCE Instance stockout errors
	var gkeResults [][]string
	var slurmResults [][]string

	filterLower := strings.ToLower(strings.TrimSpace(clusterFilter))

	processor := func(entry *logging.Entry) ([]string, bool) {
		zone := ""
		if entry.Resource != nil && entry.Resource.Labels != nil {
			zone = entry.Resource.Labels["zone"]
		}

		payloadStr := fmt.Sprintf("%v", entry.Payload)
		instanceName := ""

		// Attempt to parse instance name from protoPayload resourceName
		idx := strings.Index(payloadStr, "/instances/")
		if idx != -1 {
			sub := payloadStr[idx+len("/instances/"):]
			endIdx := strings.IndexAny(sub, " \"]}")
			if endIdx != -1 {
				instanceName = sub[:endIdx]
			} else {
				instanceName = sub
			}
		}

		return []string{entry.Timestamp.Format(time.RFC3339), zone, instanceName}, true
	}

	// Define the base query without cluster restraints
	baseQuery := fmt.Sprintf(`resource.type="gce_instance" AND protoPayload.status.message:"ZONE_RESOURCE_POOL_EXHAUSTED" AND timestamp >= "%s" AND timestamp <= "%s"`, start.Format(time.RFC3339), end.Format(time.RFC3339))

	// Dynamically inject the specific cluster name if requested by the AI
	if clusterName != "" {
		baseQuery += fmt.Sprintf(` AND protoPayload.resourceName:"%s"`, clusterName)
	}

	// Helper to execute and classify a specific cluster query
	fetchAndClassify := func(isGke bool) {
		query := baseQuery
		if isGke {
			query += ` AND protoPayload.resourceName:"gke"`
		} else {
			query += ` AND NOT protoPayload.resourceName:"gke"`
		}

		WriteToLog(fmt.Sprintf("CheckStockoutErrorsCore using filter: %s", query))
		_, results, _ := SearchLogsCore(ctx, projectID, query, 100, processor)

		for _, r := range results {
			if len(r) >= 3 {
				if isGke {
					gkeResults = append(gkeResults, r)
				} else {
					slurmResults = append(slurmResults, r)
				}
			}
		}
	}

	// Execute the queries based on the filter
	if filterLower == "gke" {
		fetchAndClassify(true)
	} else if filterLower == "slurm" {
		fetchAndClassify(false)
	} else {
		// "all" - perform both independently to return 100 of each
		fetchAndClassify(true)
		fetchAndClassify(false)
	}

	if len(gkeResults) == 0 && len(slurmResults) == 0 {
		return fmt.Sprintf("No stockout errors (ZONE_RESOURCE_POOL_EXHAUSTED) found in project %s between %s and %s for the requested filter.", projectID, start.Format("2006-01-02"), end.Format("2006-01-02")), nil
	}

	var sb strings.Builder

	service, err := compute.NewService(ctx, option.WithScopes(compute.ComputeScope))
	if err != nil {
		return fmt.Sprintf("Failed to initialize compute service: %v\n", err), nil
	}

	reportClusterType := func(clusterType string, resultsObj [][]string) {
		sb.WriteString(fmt.Sprintf("\n--- %s ---\n", clusterType))
		if len(resultsObj) == 0 {
			sb.WriteString("No stockout errors found.\n")
			return
		}

		zoneTimestamps := make(map[string][]string)
		var instancesToCheck []string

		for _, r := range resultsObj {
			ts := r[0]
			zone := r[1]
			inst := r[2]

			if zone != "" {
				zoneTimestamps[zone] = append(zoneTimestamps[zone], ts)
			}
			if inst != "" {
				alreadyAdded := false
				for _, existing := range instancesToCheck {
					if existing == inst {
						alreadyAdded = true
						break
					}
				}
				if !alreadyAdded {
					instancesToCheck = append(instancesToCheck, inst)
				}
			}
		}

		sb.WriteString(fmt.Sprintf("Found %d stockout error(s) across %d zone(s).\n", len(resultsObj), len(zoneTimestamps)))

		for zone, timestamps := range zoneTimestamps {
			sb.WriteString(fmt.Sprintf("\nZone: %s\n", zone))
			displayLimit := 10
			if len(timestamps) > displayLimit {
				sb.WriteString(fmt.Sprintf("  Error Timestamps: %s ... (and %d more)\n", strings.Join(timestamps[:displayLimit], ", "), len(timestamps)-displayLimit))
			} else {
				sb.WriteString(fmt.Sprintf("  Error Timestamps: %s\n", strings.Join(timestamps, ", ")))
			}

			// 1. Cross-reference reservations
			sb.WriteString("  Current Reservations Details:\n")
			reqRes := service.Reservations.List(projectID, zone)
			foundAnyRes := false
			_ = reqRes.Pages(ctx, func(page *compute.ReservationList) error {
				for _, res := range page.Items {
					foundAnyRes = true
					statusStr := res.Status
					if res.SpecificReservationRequired {
						statusStr += " (Specific Required)"
					}
					machineType := "Unknown"
					if res.SpecificReservation != nil && res.SpecificReservation.InstanceProperties != nil {
						machineType = GetResourceNameFromURL(res.SpecificReservation.InstanceProperties.MachineType)
					}
					sb.WriteString(fmt.Sprintf("    - Name: %s | MachineType: %s | Status: %s | Created: %s\n", res.Name, machineType, statusStr, res.CreationTimestamp))
				}
				return nil
			})
			if !foundAnyRes {
				sb.WriteString("    (No reservations found in this zone)\n")
			}
		}

		sb.WriteString(fmt.Sprintf("\nAffected Instances (%d found): %s\n", len(instancesToCheck), strings.Join(instancesToCheck, ", ")))

		if len(instancesToCheck) > 0 {
			sampleInst := instancesToCheck[0]
			sb.WriteString(fmt.Sprintf("\nChecking consumption type for the most recently affected instance: %s\n", sampleInst))

			sharedReq := CheckConsumptionRequestShared{
				InstanceName: sampleInst,
				ProjectID:    projectID,
			}

			consumptionInfo, consErr := CheckInstanceConsumptionCore(ctx, sharedReq, defaultProjectID)
			if consErr != nil {
				sb.WriteString(fmt.Sprintf("  Failed to check consumption: %v\n", consErr))
			} else {
				sb.WriteString(fmt.Sprintf("  Instance Name: %s\n", consumptionInfo.InstanceName))
				sb.WriteString(fmt.Sprintf("  Provisioning Model: %s\n", consumptionInfo.ProvisioningModel))
				sb.WriteString(fmt.Sprintf("  Reservation Affinity: %s\n", consumptionInfo.ReservationAffinity))
				sb.WriteString(fmt.Sprintf("  Consumption Status: %s\n", consumptionInfo.ConsumptionStatus))
			}
		}
	}

	if filterLower == "gke" {
		reportClusterType("GKE Clusters", gkeResults)
	} else if filterLower == "slurm" {
		reportClusterType("Slurm Clusters", slurmResults)
	} else {
		reportClusterType("GKE Clusters", gkeResults)
		reportClusterType("Slurm Clusters", slurmResults)
	}

	return sb.String(), nil
}

// CheckInternalErrorsCore executes a log search query for "Internal error"
// over a given timeframe. It extracts the affected instance names and timestamps,
// prints the reservations in the affected zone, and checks the consumption type.
func CheckInternalErrorsCore(ctx context.Context, defaultProjectID string, reqProjectID string, startDateStr string, endDateStr string, numberOfDays int, clusterFilter string, clusterName string) (string, error) {
	WriteToLog("CheckInternalErrorsCore for Internal error")

	projectID := reqProjectID
	if projectID == "" {
		projectID = defaultProjectID
	}

	if numberOfDays <= 0 {
		numberOfDays = 14
	}

	start := time.Now().AddDate(0, 0, -numberOfDays)
	end := time.Now().UTC()

	// Assuming ParseTime is available in your package as used in searchGceErrorsCommon
	if startDateStr != "" {
		if parsedStart, ok := ParseTime(startDateStr); ok {
			start = parsedStart
		}
	}
	if endDateStr != "" {
		if parsedEnd, ok := ParseTime(endDateStr); ok {
			end = parsedEnd
		}
	}

	client, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		return "", fmt.Errorf("failed to create logging client: %w", err)
	}
	defer client.Close()

	service, err := compute.NewService(ctx, option.WithScopes(compute.ComputeScope))
	if err != nil {
		return "", fmt.Errorf("failed to initialize compute service: %w", err)
	}

	var gkeResults [][]string
	var slurmResults [][]string

	filterLower := strings.ToLower(strings.TrimSpace(clusterFilter))

	// Construct the query string targeting jsonPayload and the specific error string
	queryParts := []string{
		fmt.Sprintf(`timestamp >= "%s"`, start.Format(time.RFC3339)),
		fmt.Sprintf(`timestamp <= "%s"`, end.Format(time.RFC3339)),
		`jsonPayload.message:"Internal error"`,
	}

	if clusterName != "" {
		queryParts = append(queryParts, fmt.Sprintf(`labels.cluster_name="%s"`, clusterName))
	}

	logFilter := strings.Join(queryParts, " AND ")
	WriteToLog(fmt.Sprintf("CheckInternalErrorsCore using filter: %s", logFilter))

	it := client.Entries(ctx, logadmin.Filter(logFilter))

	// Iterate logs and separate them into GKE and Slurm buckets
	for {
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", fmt.Errorf("error iterating logs: %w", err)
		}

		ts := entry.Timestamp.Format(time.RFC3339)

		// Extract zone from resource labels
		zone := ""
		if entry.Resource != nil && entry.Resource.Labels != nil {
			if z, ok := entry.Resource.Labels["zone"]; ok {
				zone = z
			}
		}

		// Extract instance hostname from labels (Slurm logs) or resource labels
		instanceName := ""
		if entry.Labels != nil {
			if host, ok := entry.Labels["hostname"]; ok {
				instanceName = host
			}
		}
		if instanceName == "" && entry.Resource != nil && entry.Resource.Labels != nil {
			if inst, ok := entry.Resource.Labels["instance_id"]; ok {
				instanceName = inst
			}
		}

		// Classify the log as GKE or Slurm
		// Slurm logs typically have 'slurmsync.log' in the path or a 'cluster_name' label
		isSlurm := false
		if entry.Labels != nil {
			if path, ok := entry.Labels["agent.googleapis.com/log_file_path"]; ok && strings.Contains(path, "slurm") {
				isSlurm = true
			}
			if _, ok := entry.Labels["cluster_name"]; ok {
				isSlurm = true
			}
		}
		isGke := !isSlurm

		r := []string{ts, zone, instanceName}

		if isGke && (filterLower == "gke" || filterLower == "all" || filterLower == "") {
			gkeResults = append(gkeResults, r)
		}
		if isSlurm && (filterLower == "slurm" || filterLower == "all" || filterLower == "") {
			slurmResults = append(slurmResults, r)
		}
	}

	if len(gkeResults) == 0 && len(slurmResults) == 0 {
		return fmt.Sprintf("No Internal errors found in project %s between %s and %s for the requested filter.", projectID, start.Format("2006-01-02"), end.Format("2006-01-02")), nil
	}

	var sb strings.Builder

	// Reusable closure to generate the output string formatted exactly like searchGceErrorsCommon
	reportClusterType := func(clusterType string, resultsObj [][]string) {
		sb.WriteString(fmt.Sprintf("\n--- %s ---\n", clusterType))
		if len(resultsObj) == 0 {
			sb.WriteString("No Internal errors found.\n")
			return
		}

		zoneTimestamps := make(map[string][]string)
		var instancesToCheck []string

		for _, r := range resultsObj {
			ts := r[0]
			zone := r[1]
			inst := r[2]

			if zone != "" {
				zoneTimestamps[zone] = append(zoneTimestamps[zone], ts)
			}
			if inst != "" {
				alreadyAdded := false
				for _, existing := range instancesToCheck {
					if existing == inst {
						alreadyAdded = true
						break
					}
				}
				if !alreadyAdded {
					instancesToCheck = append(instancesToCheck, inst)
				}
			}
		}

		sb.WriteString(fmt.Sprintf("Found %d Internal error(s) across %d zone(s).\n", len(resultsObj), len(zoneTimestamps)))

		// 1. Check if log timestamp overlaps with reservations (Display reservations per zone)
		for zone, timestamps := range zoneTimestamps {
			sb.WriteString(fmt.Sprintf("\nZone: %s\n", zone))
			displayLimit := 10
			if len(timestamps) > displayLimit {
				sb.WriteString(fmt.Sprintf("  Error Timestamps: %s ... (and %d more)\n", strings.Join(timestamps[:displayLimit], ", "), len(timestamps)-displayLimit))
			} else {
				sb.WriteString(fmt.Sprintf("  Error Timestamps: %s\n", strings.Join(timestamps, ", ")))
			}

			sb.WriteString("  Current Reservations Details:\n")
			reqRes := service.Reservations.List(projectID, zone)
			foundAnyRes := false
			_ = reqRes.Pages(ctx, func(page *compute.ReservationList) error {
				for _, res := range page.Items {
					foundAnyRes = true
					statusStr := res.Status
					if res.SpecificReservationRequired {
						statusStr += " (Specific Required)"
					}
					machineType := "Unknown"
					if res.SpecificReservation != nil && res.SpecificReservation.InstanceProperties != nil {
						machineType = GetResourceNameFromURL(res.SpecificReservation.InstanceProperties.MachineType)
					}
					sb.WriteString(fmt.Sprintf("    - Name: %s | MachineType: %s | Status: %s | Created: %s\n", res.Name, machineType, statusStr, res.CreationTimestamp))
				}
				return nil
			})
			if !foundAnyRes {
				sb.WriteString("    (No reservations found in this zone)\n")
			}
		}

		sb.WriteString(fmt.Sprintf("\nAffected Instances (%d found): %s\n", len(instancesToCheck), strings.Join(instancesToCheck, ", ")))

		// 2. Check consumption type for the hostname
		if len(instancesToCheck) > 0 {
			sampleInst := instancesToCheck[0]
			sb.WriteString(fmt.Sprintf("\nChecking consumption type for the most recently affected instance: %s\n", sampleInst))

			sharedReq := CheckConsumptionRequestShared{
				InstanceName: sampleInst,
				ProjectID:    projectID,
			}

			consumptionInfo, consErr := CheckInstanceConsumptionCore(ctx, sharedReq, defaultProjectID)
			if consErr != nil {
				sb.WriteString(fmt.Sprintf("  Failed to check consumption: %v\n", consErr))
			} else {
				sb.WriteString(fmt.Sprintf("  Instance Name: %s\n", consumptionInfo.InstanceName))
				sb.WriteString(fmt.Sprintf("  Provisioning Model: %s\n", consumptionInfo.ProvisioningModel))
				sb.WriteString(fmt.Sprintf("  Reservation Affinity: %s\n", consumptionInfo.ReservationAffinity))
				sb.WriteString(fmt.Sprintf("  Consumption Status: %s\n", consumptionInfo.ConsumptionStatus))
			}
		}
	}

	// Output Formatting Execution
	if filterLower == "gke" {
		reportClusterType("GKE Clusters", gkeResults)
	} else if filterLower == "slurm" {
		reportClusterType("Slurm Clusters", slurmResults)
	} else {
		reportClusterType("GKE Clusters", gkeResults)
		reportClusterType("Slurm Clusters", slurmResults)
	}

	return sb.String(), nil
}
