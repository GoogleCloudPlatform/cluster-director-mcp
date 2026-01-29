package cluster

import (
	"bufio"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"cluster-director-mcp/persistence"
)

// AggregationStrategy determines how result strings are compared during grouping.
type AggregationStrategy int

const (
	StrategyStrict           AggregationStrategy = iota // Exact string match.
	StrategyIgnoreWhitespace                            // Normalize whitespace before matching.
)

var hostRegex = regexp.MustCompile(`^(.*?)(\d+)$`)

// Regex to strip the "0: ", "1: " prefixes added by srun/slurm
var srunLabelRegex = regexp.MustCompile(`^\d+:\s+(.*)`)

// AnalyzeJobLog parses raw log content to determine the final status and execution result of a job.
func AnalyzeJobLog(jobType persistence.LONG_RUNNING_OPERATION, logContent string) (persistence.LONG_RUNNING_OPERATION_STATUS, persistence.LONG_RUNNING_OPERATION_EXEC_RESULT, string) {
	status := persistence.Running
	result := persistence.JOB_EXEC_RESULT_DONT_KNOW
	summary := ""

	switch jobType {
	case persistence.NCCL_TEST:
		if strings.Contains(logContent, "NCCL tests PASSED on all nodes") {
			status = persistence.Completed
			result = persistence.SUCCESS
			summary = "NCCL tests PASSED on all nodes! \n\n" + logContent
		} else if strings.Contains(logContent, "Insufficient bus bandwidth") {
			status = persistence.Completed
			result = persistence.FAIL
			summary = "NCCL tests failed: Insufficient bus bandwidth. \n\n" + logContent
		}

	case persistence.VERSION_CHECK:
		// Check for the header marker we echo in the script
		if strings.Contains(logContent, "=== HOST:") {
			status = persistence.Completed
			result = persistence.SUCCESS

			// 1. Parse the raw srun output into a map of Hostname -> Output
			nodeResults := parseVersionCheckLog(logContent)

			// 2. Aggregate identical results (groups nodes with same versions)
			aggregated := ProcessResults(nodeResults, StrategyIgnoreWhitespace, true)

			// 3. Build a clean, readable summary string
			var sb strings.Builder
			sb.WriteString("Software Version Check Completed.\n\n")

			// Sort the keys (output) so the result is deterministic
			var outputs []string
			for k := range aggregated {
				outputs = append(outputs, k)
			}
			sort.Strings(outputs)

			for _, output := range outputs {
				nodes := aggregated[output]
				sb.WriteString(fmt.Sprintf("--- Nodes: %s ---\n%s\n\n", nodes, output))
			}
			summary = sb.String()

		} else if strings.Contains(logContent, "Version Check Completed Successfully!") {
			// Fallback for older script versions
			status = persistence.Completed
			result = persistence.SUCCESS
			summary = "Version Check Completed Successfully."
		}

	case persistence.DCGM_TEST:
		if strings.Contains(logContent, "DCGM diagnostics passing on all nodes") {
			status = persistence.Completed
			result = persistence.SUCCESS
			summary = "DCGM diagnostics passing on all nodes! \n\n" + logContent
		} else if strings.Contains(logContent, "DCGM failed") {
			status = persistence.Completed
			result = persistence.FAIL
			summary = "DCGM tests failed! \n\n" + logContent
		}
	}

	return status, result, summary
}

// Splits the raw srun output (which comes interleaved) into per-host blocks
func parseVersionCheckLog(content string) map[string]string {
	results := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(content))

	var currentHost string
	var currentBuffer strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// 1. Strip the srun label (e.g. "0: Output" -> "Output")
		matches := srunLabelRegex.FindStringSubmatch(line)
		if len(matches) > 1 {
			line = matches[1]
		}

		// 2. Detect start of a new host block
		if strings.Contains(line, "=== HOST:") {
			// Save previous host's buffer
			if currentHost != "" {
				results[currentHost] = strings.TrimSpace(currentBuffer.String())
			}

			// Extract new hostname "=== HOST: node-1 ==="
			parts := strings.Split(line, "HOST:")
			if len(parts) > 1 {
				currentHost = strings.TrimSpace(strings.TrimSuffix(parts[1], "==="))
			}
			currentBuffer.Reset()
			continue
		}

		// 3. Filter noise/headers
		if strings.Contains(line, "==========================") || strings.TrimSpace(line) == "" {
			continue
		}

		if currentHost != "" {
			currentBuffer.WriteString(line + "\n")
		}
	}

	// Save the last block
	if currentHost != "" {
		results[currentHost] = strings.TrimSpace(currentBuffer.String())
	}

	return results
}

// ProcessResults groups node results based on the provided strategy and optionally compresses hostnames.
func ProcessResults(nodeResults map[string]string, strategy AggregationStrategy, compressHostnames bool) map[string]string {
	buckets := bucketResults(nodeResults, strategy)
	finalOutput := make(map[string]string)

	for resultKey, nodeList := range buckets {
		if compressHostnames {
			finalOutput[resultKey] = CompressHostnames(nodeList)
		} else {
			sort.Strings(nodeList)
			finalOutput[resultKey] = strings.Join(nodeList, ",")
		}
	}
	return finalOutput
}

// bucketResults groups nodes by their result string.
// Note: It returns Map[Output String] -> [List of Nodes]
func bucketResults(nodeResults map[string]string, strategy AggregationStrategy) map[string][]string {
	buckets := make(map[string][]string)

	for node, rawResult := range nodeResults {
		key := rawResult
		if strategy == StrategyIgnoreWhitespace {
			key = strings.Join(strings.Fields(rawResult), " ")
		}
		buckets[key] = append(buckets[key], node)
	}
	return buckets
}

// CompressHostnames formats a list of hostnames into Slurm-style node range expressions (e.g., node[1-5]).
func CompressHostnames(hosts []string) string {
	if len(hosts) == 0 {
		return ""
	}

	groups := make(map[string][]int)
	var singles []string

	for _, h := range hosts {
		matches := hostRegex.FindStringSubmatch(h)
		if matches == nil {
			singles = append(singles, h)
			continue
		}
		prefix := matches[1]
		if num, err := strconv.Atoi(matches[2]); err == nil {
			groups[prefix] = append(groups[prefix], num)
		} else {
			singles = append(singles, h)
		}
	}

	var resultParts []string
	sort.Strings(singles)
	resultParts = append(resultParts, singles...)

	var prefixes []string
	for p := range groups {
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)

	for _, p := range prefixes {
		nums := groups[p]
		sort.Ints(nums)
		rangeStr := buildRangeString(nums)
		resultParts = append(resultParts, fmt.Sprintf("%s[%s]", p, rangeStr))
	}

	return strings.Join(resultParts, ",")
}

func buildRangeString(nums []int) string {
	if len(nums) == 0 {
		return ""
	}

	var ranges []string
	start := nums[0]
	prev := nums[0]

	for i := 1; i < len(nums); i++ {
		curr := nums[i]
		if curr == prev+1 {
			prev = curr
		} else {
			ranges = append(ranges, formatSingleRange(start, prev))
			start = curr
			prev = curr
		}
	}
	ranges = append(ranges, formatSingleRange(start, prev))
	return strings.Join(ranges, ",")
}

func formatSingleRange(start, end int) string {
	if start == end {
		return strconv.Itoa(start)
	}
	return fmt.Sprintf("%d-%d", start, end)
}
