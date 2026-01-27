package cluster

import (
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
			summary = "NCCL tests PASSED on all nodes!"
		} else if strings.Contains(logContent, "Insufficient bus bandwidth") {
			status = persistence.Completed
			result = persistence.FAIL
			summary = "NCCL tests failed: Insufficient bus bandwidth."
		}

	case persistence.VERSION_CHECK:
		if strings.Contains(logContent, "Version Check Completed Successfully!") {
			status = persistence.Completed
			result = persistence.SUCCESS
			summary = "Version Check Completed Successfully.\n\n" + logContent
		} else if strings.Contains(logContent, "=== HOST:") {
			status = persistence.Completed
			result = persistence.SUCCESS
			summary = "Version Check output received. \n\n" + logContent
		}

	case persistence.DCGM_TEST:
		if strings.Contains(logContent, "DCGM diagnostics passing on all nodes") {
			status = persistence.Completed
			result = persistence.SUCCESS
			summary = "DCGM diagnostics passing on all nodes!"
		} else if strings.Contains(logContent, "DCGM failed") {
			status = persistence.Completed
			result = persistence.FAIL
			summary = "DCGM tests failed!"
		}
	}

	return status, result, summary
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

// bucketResults groups nodes by their result string using the specified aggregation strategy.
func bucketResults(nodeResults map[string]string, strategy AggregationStrategy) map[string][]string {
	buckets := make(map[string][]string)

	for node, rawResult := range nodeResults {
		key := rawResult
		if strategy == StrategyIgnoreWhitespace {
			key = strings.Join(strings.Fields(rawResult), "")
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

// buildRangeString converts a sorted slice of integers into a concise range string.
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
