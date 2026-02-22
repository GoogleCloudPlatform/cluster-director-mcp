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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
)

const maxLogFiles = 100

var logger *slog.Logger

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

func WriteToLog(message string) {

	message = strings.ReplaceAll(message, "\n", " | ")

	if logger == nil {
		f := CreateUniqueFilePath("logs/log.cluster-director-mcp")
		var writer io.Writer
		if f != nil {
			writer = f
		} else {
			writer = os.Stdout
		}

		opts := &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelInfo,
		}

		// Initialize the custom handler
		handler := &PlainHandler{
			w:    writer,
			opts: *opts,
		}

		logger = slog.New(handler)
		slog.SetDefault(logger)
	}

	// 1. Capture the Program Counter (PC) of the caller
	// We skip 2 frames:
	// 0 = runtime.Callers
	// 1 = WriteToLog
	// 2 = The function calling WriteToLog (e.g., clusterCore.go)
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])

	// 2. Create the record with the specific PC
	r := slog.NewRecord(time.Now(), slog.LevelInfo, message, pcs[0])

	// 3. Handle the record
	_ = logger.Handler().Handle(context.Background(), r)
}

type PlainHandler struct {
	w    io.Writer
	opts slog.HandlerOptions
}

// Enabled reports whether the handler handles records at the given level.
func (h *PlainHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.opts.Level.Level()
}

func ParseTime(dateStr string) (time.Time, bool) {

	layouts := []string{
		time.RFC3339,       // ISO 8601
		"2006-01-02",       // YYYY-MM-DD
		"01/02/2006",       // MM/DD/YYYY
		"02-01-2006 15:04", // DD-MM-YYYY HH:MM
		"Jan 2, 2006",      // Month Day, Year
		"02 Jan 2006",      // Date (DD Mon YYYY) e.g., "25 Oct 2023"
		"Jan 2",            // Date, Month (Mon DD) e.g., "Oct 25"
		"02",               // Just the day
	}

	parsedTime, formatUsed, err := parseWithFallback(dateStr, layouts)
	if err != nil {
		WriteToLog("Could not parse date string: " + dateStr)
		return time.Now(), false
	}

	// Post-processing: Infer missing data based on the format used
	now := time.Now()

	switch formatUsed {
	case "02":
		// Case: User gave only "Day". Use Current Year and Current Month.
		parsedTime = time.Date(now.Year(), now.Month(), parsedTime.Day(), 0, 0, 0, 0, time.Local)

	case "Jan 2":
		// Case: User gave "Month Day". Use Current Year.
		parsedTime = parsedTime.AddDate(now.Year(), 0, 0)
	}
	WriteToLog(fmt.Sprintf("Successfully parsed input date string %s \nParsed Time: %v\nFormat Used: %s\n", dateStr, parsedTime, formatUsed))

	return parsedTime, true
}

func parseWithFallback(input string, formats []string) (time.Time, string, error) {
	for _, layout := range formats {
		t, err := time.Parse(layout, input)
		if err == nil {
			return t, layout, nil
		}
	}
	return time.Time{}, "", errors.New("no matching time format found")
}

// Handle formats the record as a plain string without keys
func (h *PlainHandler) Handle(ctx context.Context, r slog.Record) error {
	// 1. Format Time
	timeStr := r.Time.Format(time.RFC3339)

	// 2. Format Source (File:Line)
	sourceStr := ""
	if h.opts.AddSource && r.PC != 0 {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		f, _ := fs.Next()
		sourceStr = fmt.Sprintf("%s:%d", f.File, f.Line)
	}

	// 3. Format Level
	levelStr := r.Level.String()

	// 4. Construct the final string: "TIME LEVEL SOURCE MESSAGE"
	_, err := fmt.Fprintf(h.w, "%s %s %s %s\n", timeStr, levelStr, sourceStr, r.Message)
	return err
}

func (h *PlainHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }

func (h *PlainHandler) WithGroup(name string) slog.Handler { return h }

// SearchByColumn1 searches for a target string in the second column (index 1).
// It returns the found row and true, or nil and false if not found.
func SearchByColumn1(data [][]string, target string) ([]string, bool) {
	for _, row := range data {
		// SAFETY CHECK: Ensure the row has at least 2 columns (indices 0 and 1)
		// If we don't check this, a short row will cause a "panic: index out of range"
		if len(row) > 1 {

			// Option A: Exact Match (Case-Sensitive)
			if row[1] == target {
				return row, true
			}

			// Option B: Case-Insensitive Match (Uncomment to use)
			// if strings.EqualFold(row[1], target) {
			// 	return row, true
			// }
		}
	}
	return nil, false
}

// getLastLines scans the string and keeps a rolling slice of the last n lines.
func GetLastLines(s string, n int) string {
	var lines []string

	// Use a scanner to read the string line by line
	scanner := bufio.NewScanner(strings.NewReader(s))
	for scanner.Scan() {
		// Append the new line
		lines = append(lines, scanner.Text())

		// If we have more than n lines, drop the oldest one (at the front)
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	// We ignore scanner.Err() for this example

	// Join the remaining lines back together
	return strings.Join(lines, "\n")
}

func DeleteFile(filePathName string) bool {
	err := os.Remove(filePathName)
	if err != nil {
		WriteToLog(fmt.Sprintf("Failed to delete file: %s", filePathName))
		return false
	}
	return true
}

// dirExists checks if a directory exists at the given path.
func CheckFileOrDirExists(path string, checkIfItsDir bool) bool {
	// 1. Get FileInfo for the path.
	info, err := os.Stat(path)

	if err == nil {
		// 2. Path exists. Check if it's a directory.
		if checkIfItsDir {
			if info.IsDir() {
				WriteToLog(fmt.Sprintf("Directory exists: %s", path))
				return true
			}

			// Path exists but is a file, not a directory.
			WriteToLog(fmt.Sprintf("Path exists, but its not a directory: %s", path))
			return false
		}
		// Its a file and it exists
		WriteToLog(fmt.Sprintf("Path exists, its a file: %s", path))
		return true
	}

	// 3. Path does not exist.
	if os.IsNotExist(err) {
		WriteToLog(fmt.Sprintf("File or Directory does NOT exist: %s", path))
		return false
	}

	WriteToLog(fmt.Sprintf("Cannot determine if directory exists: %s", path))

	// 4. A different error occurred (e.g., permission issue).
	return false
}

func getUniqueLogFileName(logNameRoot string) string {
	for i := 0; i < maxLogFiles; i++ {
		_, err := os.Stat(fmt.Sprintf("%s.%d", logNameRoot, i))
		if err != nil && !os.IsNotExist(err) {
			return fmt.Sprintf("%s.%d", logNameRoot, i)
		}
	}

	return fmt.Sprintf("%s.%d", logNameRoot, 0)
}

func CreateUniqueFilePath(logNameRoot string) *os.File {
	// Make the directory if it does not exist, fail silently
	_ = os.MkdirAll(filepath.Dir(logNameRoot), 0755)
	logFile, err := os.OpenFile(getUniqueLogFileName(logNameRoot), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		// If we can't open the log file, it's a fatal error, so we exit.
		return nil
	}

	// logFile is intentionally not closed - its kept open
	return logFile
}

func QueryURLAndGetResult(authToken string, url string) (string, bool) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		WriteToLog(fmt.Sprintf("Could create HTTP request object to to URL: %s", url))
		return "", false
	}

	req.Header.Set("Content-Type", "application/json")
	authHeader := fmt.Sprintf("Bearer %s", authToken)
	req.Header.Set("Authorization", authHeader)
	client := &http.Client{
		Timeout: 30 * time.Second, // Set a reasonable timeout.
	}

	resp, err := client.Do(req)
	if err != nil {
		WriteToLog("Could not making HTTP request to URL: " + url)
		return "", false
	}
	// Defer the closing of the response body.
	// This is important to free up network resources.
	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		WriteToLog("http.Get() did NOT return StatusOK")
		return "", false
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		WriteToLog("io.ReadAll(body) returned error. Returning ERROR")
		return "", false
	}

	bodyString := string(body)
	return bodyString, true
}

// containsAny checks if a string contains any of the substrings.
func StringMatchesAnySubstring(s string, substrings []string) bool {
	for _, sub := range substrings {
		if strings.Contains(s, sub) {
			return true // Found a match
		}
	}
	return false // No matches found
}

// contains checks if an integer is present in a slice.
func IntArrContains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
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
func FindInstanceInProject(ctx context.Context, service *compute.Service, projectID, instanceName string) (*compute.Instance, string, error) {
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
func ProcessConsumptionRequest(ctx context.Context, instanceNames []string, zone, reqProjectID, defaultProjectID string) (string, error) {
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

const LOCAL_HOST_SCRATCH_DIR = "cluster-director-mcp.scratch"

func CreateScratchDir() bool {
	if CheckFileOrDirExists(LOCAL_HOST_SCRATCH_DIR, true) {
		return true
	}
	err := os.MkdirAll(LOCAL_HOST_SCRATCH_DIR, 0755)
	if err != nil {
		WriteToLog(fmt.Sprintf("Failed to create scratch directory: %s %v", LOCAL_HOST_SCRATCH_DIR, err))
		return false
	}
	return true
}

func SlurpFile(fileName string) (string, error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// CheckStockoutErrorsCore executes a log search query for ZONE_RESOURCE_POOL_EXHAUSTED
// over a given timeframe. It extracts the affected instance names and timestamps, cross-references
// reservations, and checks the consumption type of at least one instance.
func CheckStockoutErrorsCore(ctx context.Context, defaultProjectID, reqProjectID, startDateStr, endDateStr string, numberOfDays int, clusterFilter string) (string, error) {
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
	filter := fmt.Sprintf(`resource.type="gce_instance" AND protoPayload.status.message:"ZONE_RESOURCE_POOL_EXHAUSTED" AND timestamp >= "%s" AND timestamp <= "%s"`, start.Format(time.RFC3339), end.Format(time.RFC3339))

	WriteToLog(fmt.Sprintf("CheckStockoutErrorsCore using filter: %s", filter))

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

	// Fetch up to 100 recent stockout errors
	_, results, success := SearchLogsCore(ctx, projectID, filter, 100, processor)

	if !success || len(results) == 0 {
		return fmt.Sprintf("No stockout errors (ZONE_RESOURCE_POOL_EXHAUSTED) found in project %s between %s and %s.", projectID, start.Format("2006-01-02"), end.Format("2006-01-02")), nil
	}

	var sb strings.Builder
	
	service, err := compute.NewService(ctx, option.WithScopes(compute.ComputeScope))
	if err != nil {
		return fmt.Sprintf("Failed to initialize compute service: %v\n", err), nil
	}

	var gkeResults [][]string
	var slurmResults [][]string

	for _, r := range results {
		if len(r) >= 3 {
			inst := r[2]
			if strings.Contains(strings.ToLower(inst), "gke") {
				gkeResults = append(gkeResults, r)
			} else {
				slurmResults = append(slurmResults, r)
			}
		}
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
			sb.WriteString(fmt.Sprintf("  Error Timestamps: %s\n", strings.Join(timestamps, ", ")))

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
					sb.WriteString(fmt.Sprintf("    - Name: %s | MachineType: %s | Status: %s\n", res.Name, machineType, statusStr))
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
			sb.WriteString(fmt.Sprintf("\nChecking consumption type for sample affected instance: %s\n", sampleInst))
			
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

	filterLower := strings.ToLower(strings.TrimSpace(clusterFilter))
	
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
