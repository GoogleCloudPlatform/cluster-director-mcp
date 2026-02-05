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
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"google.golang.org/api/compute/v1"
)

const maxLogFiles = 100

var logger *slog.Logger

// ListReservationsRequest represents the input schema for the list_reservations tool.
type ListReservationsRequest struct {
	ProjectID string `json:"projectId,omitempty" jsonschema:"description=GCP project ID. Optional."`
	Zone      string `json:"zone,omitempty" jsonschema:"description=GCP zone. Optional."`
}

// GcloudListItem represents a single item from the gcloud list command's JSON output.
type GcloudListItem struct {
	Name string `json:"name"`
}

func WriteToLog(message string) {

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
func RunGcloudListCommand(resource string) ([]string, error) {
	cmd := exec.Command("gcloud", "compute", resource, "list", "--format=json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("gcloud command for %s failed: %w", resource, err)
	}

	var items []GcloudListItem
	if err := json.Unmarshal(output, &items); err != nil {
		return nil, fmt.Errorf("failed to parse gcloud output for %s: %w", resource, err)
	}

	names := make([]string, len(items))
	for i, item := range items {
		names[i] = item.Name
	}

	return names, nil
}

// GetGCloudRegionsAndZones fetches all available GCP regions and zones using the gcloud CLI.
func GetGCloudRegionsAndZones() ([]string, []string, error) {
	regions, err := RunGcloudListCommand("regions")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get regions: %w", err)
	}

	zones, err := RunGcloudListCommand("zones")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get zones: %w", err)
	}

	return regions, zones, nil
}

// ListReservationsCore fetches reservations for a given project and zone using the Compute API.
func ListReservationsCore(ctx context.Context, projectID string, zone string) (string, error) {
	service, err := compute.NewService(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create compute service: %v", err)
	}

	var result strings.Builder
	hasItems := false
	itemCount := 1

	req := service.Reservations.List(projectID, zone)
	err = req.Pages(ctx, func(page *compute.ReservationList) error {
		for _, res := range page.Items {
			if !hasItems {
				result.WriteString(fmt.Sprintf("Zone: %s\n", zone))
				hasItems = true
				result.WriteString("------------------------------------------------\n")
			}
			result.WriteString(fmt.Sprintf("%d. Name: %s\n", itemCount, res.Name))
			itemCount++
		}
		if hasItems {
			result.WriteString("------------------------------------------------\n")
		}
		return nil
	})

	if err != nil {
		return "", fmt.Errorf("error iterating listing reservations in zone %s: %v", zone, err)
	}

	if !hasItems {
		return "", nil
	}

	return result.String(), nil
}

// ListReservationsMCP provides the high-level logic for the list_reservations tool.
func ListReservationsMCP(ctx context.Context, projectID string, zone string) (string, error) {
	if projectID == "" {
		return "Could not determine GCP project. Please run: gcloud config set project \"your-project-name\" and restart the AI Assistant", nil
	}

	if zone != "" {
		resInfo, err := ListReservationsCore(ctx, projectID, zone)
		if err != nil {
			return "", err
		}
		if resInfo == "" {
			return fmt.Sprintf("No reservations found in zone %s of project %s.", zone, projectID), nil
		}
		return resInfo, nil
	}

	// If zone is not given, use AggregatedList to find all reservations in the project
	service, err := compute.NewService(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create compute service: %v", err)
	}

	var result strings.Builder
	result.WriteString(fmt.Sprintf("Listing reservations for all zones in project %s:\n\n", projectID))

	foundAny := false
	req := service.Reservations.AggregatedList(projectID)
	err = req.Pages(ctx, func(page *compute.ReservationAggregatedList) error {
		for zoneKey, scopedList := range page.Items {
			if len(scopedList.Reservations) == 0 {
				continue
			}

			// zoneKey is usually "zones/us-central1-a"
			zoneName := zoneKey
			if strings.HasPrefix(zoneKey, "zones/") {
				zoneName = strings.TrimPrefix(zoneKey, "zones/")
			}

			result.WriteString(fmt.Sprintf("Zone: %s\n", zoneName))
			result.WriteString("------------------------------------------------\n")
			for i, res := range scopedList.Reservations {
				result.WriteString(fmt.Sprintf("%d. Name: %s\n", i+1, res.Name))
			}
			result.WriteString("------------------------------------------------\n\n")
			foundAny = true
		}
		return nil
	})

	if err != nil {
		return "", fmt.Errorf("error listing aggregated reservations: %v", err)
	}

	if !foundAny {
		return fmt.Sprintf("No reservations found in any zone of project %s.", projectID), nil
	}

	return result.String(), nil
}

// GetMachinesInReservationRequest represents the input for the tool
type GetMachinesInReservationRequest struct {
	ProjectID       string `json:"projectId,omitempty" jsonschema:"description=GCP project ID. Optional."`
	Zone            string `json:"zone,omitempty" jsonschema:"description=GCP zone. Optional."`
	ReservationName string `json:"reservationName,omitempty" jsonschema:"description=Name of the reservation. Optional."`
}

// ReservationData combines the Official Schema with your Custom Metrics
type ReservationData struct {
	// Embed the Official Google Cloud Struct (Matches Output Schema Exactly)
	*compute.Reservation

	// Your Custom Calculated Metrics
	TotalSlots int
	ActiveVms  int
	IdleVms    int
	Nodes      []string
}

// GetResourceNameFromURL extracts the last part of a GCP resource URL
func GetResourceNameFromURL(url string) string {
	if url == "" {
		return ""
	}
	parts := strings.Split(url, "/")
	return parts[len(parts)-1]
}

// GetMachinesInReservationMCP acts as the high-level handler for the tool.
func GetMachinesInReservationMCP(ctx context.Context, defaultProjectID string, req GetMachinesInReservationRequest) (string, error) {
	projectID := req.ProjectID
	if projectID == "" {
		projectID = defaultProjectID
	}
	if projectID == "" {
		return "", fmt.Errorf("could not determine GCP project. Please specify projectId or ensure gcloud is configured")
	}
	return GetMachinesInReservationCore(ctx, projectID, req.Zone, req.ReservationName)
}

// GetMachinesInReservationCore finds VMs consuming reservations and returns a detailed TEXT report matching the schema.
func GetMachinesInReservationCore(ctx context.Context, projectID, zone, resName string) (string, error) {
	service, err := compute.NewService(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create compute service: %v", err)
	}

	// Fetch Reservations
	aggRes, err := service.Reservations.AggregatedList(projectID).Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("could not list reservations: %v", err)
	}

	// Fetch Instances
	aggInstances, err := service.Instances.AggregatedList(projectID).Filter("status != TERMINATED").Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("could not list instances: %v", err)
	}

	var allData []ReservationData
	foundAny := false

	for zoneKey, scopedResList := range aggRes.Items {
		currentZone := strings.TrimPrefix(zoneKey, "zones/")
		if zone != "" && currentZone != zone {
			continue
		}

		var zoneInstances []*compute.Instance
		if item, ok := aggInstances.Items[zoneKey]; ok {
			zoneInstances = item.Instances
		}

		for _, res := range scopedResList.Reservations {
			if resName != "" && res.Name != resName {
				continue
			}
			foundAny = true
			data := buildDataFromReservation(currentZone, res, zoneInstances)
			allData = append(allData, data)
		}
	}

	if !foundAny {
		return fmt.Sprintf("No reservations found matching scope (Project: %s, Zone: %s, Name: %s).", projectID, zone, resName), nil
	}
	var report strings.Builder
	report.WriteString(fmt.Sprintf("Reservation Report for Project: %s\n", projectID))
	report.WriteString("================================================================================\n")

	for _, d := range allData {
		report.WriteString(fmt.Sprintf("Name:             %s\n", d.Name))
		report.WriteString(fmt.Sprintf("ID:               %d\n", d.Id))
		report.WriteString(fmt.Sprintf("Kind:             %s\n", d.Kind))
		report.WriteString(fmt.Sprintf("Zone:             %s\n", GetResourceNameFromURL(d.Zone)))
		report.WriteString(fmt.Sprintf("Status:           %s\n", d.Status))
		report.WriteString(fmt.Sprintf("Created:          %s\n", d.CreationTimestamp))
		report.WriteString(fmt.Sprintf("SelfLink:         %s\n", d.SelfLink))
		if d.Description != "" {
			report.WriteString(fmt.Sprintf("Description:      %s\n", d.Description))
		}
		report.WriteString(fmt.Sprintf("Utilization:      Total: %d | Active: %d | Idle: %d\n", d.TotalSlots, d.ActiveVms, d.IdleVms))
		if len(d.Nodes) > 0 {
			report.WriteString(fmt.Sprintf("Active Nodes:     %s\n", strings.Join(d.Nodes, ", ")))
		}
		report.WriteString(fmt.Sprintf("Specific Res Req: %v\n", d.SpecificReservationRequired))
		if d.Commitment != "" {
			report.WriteString(fmt.Sprintf("Commitment:       %s\n", GetResourceNameFromURL(d.Commitment)))
		}
		if len(d.LinkedCommitments) > 0 {
			report.WriteString(fmt.Sprintf("Linked Commit:    %v\n", d.LinkedCommitments))
		}
		if d.SatisfiesPzs {
			report.WriteString("Satisfies PZS:    true\n")
		}
		if d.SpecificReservation != nil && d.SpecificReservation.InstanceProperties != nil {
			props := d.SpecificReservation.InstanceProperties
			report.WriteString(fmt.Sprintf("Machine Type:     %s\n", GetResourceNameFromURL(props.MachineType)))

			if len(props.GuestAccelerators) > 0 {
				var accs []string
				for _, a := range props.GuestAccelerators {
					accs = append(accs, fmt.Sprintf("%s (x%d)", GetResourceNameFromURL(a.AcceleratorType), a.AcceleratorCount))
				}
				report.WriteString(fmt.Sprintf("Accelerators:     %s\n", strings.Join(accs, ", ")))
			}
			if len(props.LocalSsds) > 0 {
				report.WriteString(fmt.Sprintf("Local SSDs:       %d attached\n", len(props.LocalSsds)))
			}
			if props.MinCpuPlatform != "" {
				report.WriteString(fmt.Sprintf("Min CPU Plat:     %s\n", props.MinCpuPlatform))
			}
		}
		if d.AggregateReservation != nil {
			report.WriteString(fmt.Sprintf("Agg. VM Family:   %s\n", d.AggregateReservation.VmFamily))
			report.WriteString(fmt.Sprintf("Agg. Workload:    %s\n", d.AggregateReservation.WorkloadType))
		}
		if d.ShareSettings != nil {
			report.WriteString(fmt.Sprintf("Share Type:       %s\n", d.ShareSettings.ShareType))
			if d.ShareSettings.ProjectMap != nil {
				var projects []string
				for k := range d.ShareSettings.ProjectMap {
					projects = append(projects, k)
				}
				report.WriteString(fmt.Sprintf("Shared With:      %s\n", strings.Join(projects, ", ")))
			}
		}
		if d.ReservationSharingPolicy != nil {
			report.WriteString(fmt.Sprintf("Service Sharing:  %s\n", d.ReservationSharingPolicy.ServiceShareType))
		}
		if len(d.ResourcePolicies) > 0 {
			report.WriteString(fmt.Sprintf("Resource Policies:%v\n", d.ResourcePolicies))
		}
		if d.DeploymentType != "" {
			report.WriteString(fmt.Sprintf("Deployment Type:  %s\n", d.DeploymentType))
		}
		if d.AdvancedDeploymentControl != nil {
			report.WriteString(fmt.Sprintf("Adv. Deploy Mode: %s\n", d.AdvancedDeploymentControl.ReservationOperationalMode))
		}
		if d.EnableEmergentMaintenance {
			report.WriteString("Emergent Maint:   Allowed\n")
		}
		if d.ProtectionTier != "" {
			report.WriteString(fmt.Sprintf("Protection Tier:  %s\n", d.ProtectionTier))
		}
		if d.SchedulingType != "" {
			report.WriteString(fmt.Sprintf("Scheduling Type:  %s\n", d.SchedulingType))
		}
		if d.ResourceStatus != nil {
			if d.ResourceStatus.HealthInfo != nil {
				report.WriteString(fmt.Sprintf("Health Status:    %s\n", d.ResourceStatus.HealthInfo.HealthStatus))
			}
			if d.ResourceStatus.ReservationMaintenance != nil {
				report.WriteString(fmt.Sprintf("Maint. Ongoing:   %d hosts\n", d.ResourceStatus.ReservationMaintenance.MaintenanceOngoingCount))
			}
		}
		if d.DeleteAtTime != "" {
			report.WriteString(fmt.Sprintf("Auto Delete At:   %s\n", d.DeleteAtTime))
		}
		if d.DeleteAfterDuration != nil {
			report.WriteString(fmt.Sprintf("Auto Delete In:   %d sec\n", d.DeleteAfterDuration.Seconds))
		}

		report.WriteString("--------------------------------------------------------------------------------\n")
	}

	return report.String(), nil
}

// buildDataFromReservation calculates metrics and wraps the official schema
func buildDataFromReservation(zone string, res *compute.Reservation, instances []*compute.Instance) ReservationData {
	var resMachineType string
	var totalSlots int

	if res.SpecificReservation != nil {
		resMachineType = GetResourceNameFromURL(res.SpecificReservation.InstanceProperties.MachineType)
		totalSlots = int(res.SpecificReservation.Count)
	}

	activeVms := 0
	var vmNames []string

	for _, instance := range instances {
		vmType := GetResourceNameFromURL(instance.MachineType)
		isMatch := false

		if res.SpecificReservationRequired {
			if instance.ReservationAffinity != nil && instance.ReservationAffinity.ConsumeReservationType == "SPECIFIC_RESERVATION" {
				for _, val := range instance.ReservationAffinity.Values {
					if val == res.Name {
						isMatch = true
						break
					}
				}
			}
		} else if vmType == resMachineType {
			if instance.ReservationAffinity == nil || instance.ReservationAffinity.ConsumeReservationType == "ANY_RESERVATION" {
				isMatch = true
			}
		}

		if isMatch {
			activeVms++
			vmNames = append(vmNames, instance.Name)
		}
	}
	idleVms := totalSlots - activeVms

	return ReservationData{
		Reservation: res,
		TotalSlots:  totalSlots,
		ActiveVms:   activeVms,
		IdleVms:     idleVms,
		Nodes:       vmNames,
	}
}
