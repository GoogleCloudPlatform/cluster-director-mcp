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

package persistence

//-------------------------------------------------------------------------
// This package provides a light weight persistance layer to store information
// about the user's cluster-director-mcp session. Initial implementation stores data
// in ~/.cluster-director-mcp/
//-------------------------------------------------------------------------
import (
	"cluster-director-mcp/pkg/genericCore"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

const UNDEFINED_VAL = -1

// We can only report jobs submitted within this window
const JOB_EXPIRY_TIME_WINDOW time.Duration = 4 * time.Hour

// Long running operation
type LONG_RUNNING_OPERATION_EXEC_RESULT int

// The list of long running operation
const (
	SUCCESS                   LONG_RUNNING_OPERATION_EXEC_RESULT = iota // 0
	FAIL                                                                // 1
	JOB_EXEC_RESULT_DONT_KNOW                                           // 2
)

// Long running operation
type LONG_RUNNING_OPERATION_STATUS int

// The list of long running operation
const (
	Running           LONG_RUNNING_OPERATION_STATUS = iota // 0
	Completed                                              // 1
	JobStatusDontKnow                                      // 2
	JobNotPresentInSlurm
)

// Long running operation
type LONG_RUNNING_OPERATION int

// The list of long running operation
const (
	NCCL_TEST LONG_RUNNING_OPERATION = iota // 0
	DCGM_TEST                               // 1
	VERSION_CHECK						   // 2	
)

type LongRunningJob struct {
	CDMcpJobId                        int
	SlurmJobId                        int
	ProjectId                         string
	ClusterName                       string
	LoginNodeName                     string
	Zone                              string
	StartTime                         time.Time
	StartTimeHumanReadable            string
	EndTime                           time.Time
	EndTimeHumanReadable              string
	EstimatedEndTime                  time.Time
	LastStatusCheckTime               time.Time
	LastStatusCheckTimeHumanReadable  string
	LastStatusUpdateTime              time.Time
	LastStatusUpdateTimeHumanReadable string
	JobType                           LONG_RUNNING_OPERATION
	machineType                       string
	partitionName                     string
	JobStatus                         LONG_RUNNING_OPERATION_STATUS
	JobExecutionResult                LONG_RUNNING_OPERATION_EXEC_RESULT
	JobExecutionResultString          string
	RunDir                            string
	RunScriptName                     string
	SummaryLogFilePath                string
	FullLogFilePath                   string
}

var AllLongRunningJobs []LongRunningJob

var persistenceRootDir string
var jobsFile string

// Indicates if persistence layer is ready to accept requests to write data
var persistenceLayerReady = false

func initPersistenceLayerCore() bool {
	if !persistenceLayerReady {
		if CreatePersistenceRootDir() && CreateJobPersistenceFile() {
			persistenceLayerReady = true
			return true
		} else {
			persistenceLayerReady = false
			return false
		}
	}

	return true
}

func initPersistenceLayer() bool {
	success := initPersistenceLayerCore()
	if !success {
		return false
	}

	var retVal bool = false
	AllLongRunningJobs, retVal = readJobData()

	return retVal
}

func CreatePersistenceRootDir() bool {

	persistenceRootDir, _ = os.UserHomeDir()
	persistenceRootDir += "/.cluster-director-mcp"
	jobsFile = persistenceRootDir + "/jobs_history.json"

	if genericCore.CheckFileOrDirExists(persistenceRootDir, true) {
		return true
	}

	err := os.MkdirAll(persistenceRootDir, 0755)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Failed to create persistence directory: %v", err))
		return false
	}

	genericCore.WriteToLog(fmt.Sprintf("Successfully created persistence directory: %s", persistenceRootDir))
	return true
}

func CreateJobPersistenceFile() bool {
	// 1. Check if the file exists
	_, err := os.Stat(jobsFile)

	// 2. Check if the error is "file not found"
	if os.IsNotExist(err) {
		genericCore.WriteToLog(fmt.Sprintf("cluster-director-mcp job history file does not exist. Creating: %s", jobsFile))

		// 3. Create the file
		// os.Create() creates a file and opens it for writing.
		// We'll close it immediately since we just want to create it.
		file, createErr := os.Create(jobsFile)
		if createErr != nil {
			genericCore.WriteToLog(fmt.Sprintf("Failed to create cluster-director-mcp job history file: %v", createErr))
		}
		file.Close() // Don't forget to close the file

		genericCore.WriteToLog(fmt.Sprintf("cluster-director-mcp job history file created successfully : %s", jobsFile))
		return true
	} else if err != nil {
		// 4. A different error occurred (e.g., permission denied)
		genericCore.WriteToLog(fmt.Sprintf("Error checking if cluster-director-mcp job history file exists %s : %v", jobsFile, err))
		return false
	} else {
		// 5. File already exists
		genericCore.WriteToLog(fmt.Sprintf("cluster-director-mcp job history file already exists: %s", jobsFile))
	}

	return true
}

func GetJobTypeString(ii int) string {
	if AllLongRunningJobs[ii].JobType == NCCL_TEST {
		return " NCCL test "
	} else if AllLongRunningJobs[ii].JobType == DCGM_TEST {
		return " DCGM test "
	} else if AllLongRunningJobs[ii].JobType == VERSION_CHECK {
		return " Version Check "
	} else {
		return " Unknown Job Type "
	}
}

var writeMutex sync.Mutex

func WriteAllJobData() bool {
	// RAII - golang style
	writeMutex.Lock()
	defer writeMutex.Unlock()

	// Nothing to write
	if !persistenceLayerReady {
		return true
	}

	// iterate over time and create human friendly formats
	for i := range AllLongRunningJobs {
		if AllLongRunningJobs[i].StartTimeHumanReadable == "" {
			AllLongRunningJobs[i].StartTimeHumanReadable = AllLongRunningJobs[i].StartTime.Format(time.RFC1123)
		}
		if AllLongRunningJobs[i].EndTimeHumanReadable == "" {
			AllLongRunningJobs[i].EndTimeHumanReadable = AllLongRunningJobs[i].EndTime.Format(time.RFC1123)
		}
		if AllLongRunningJobs[i].LastStatusCheckTimeHumanReadable == "" {
			AllLongRunningJobs[i].LastStatusCheckTimeHumanReadable = AllLongRunningJobs[i].LastStatusCheckTime.Format(time.RFC1123)
		}
		if AllLongRunningJobs[i].LastStatusUpdateTimeHumanReadable == "" {
			AllLongRunningJobs[i].LastStatusUpdateTimeHumanReadable = AllLongRunningJobs[i].LastStatusUpdateTime.Format(time.RFC1123)
		}
	}

	jsonMarshalled, err := json.MarshalIndent(AllLongRunningJobs, "", "  ")
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error marshaling JSON: %v", err))
		return false
	}

	err = os.WriteFile(jobsFile, jsonMarshalled, 0644)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error writing to file: %v", err))
	}

	return true
}

func AppendNewJobDataAndWriteJobDataToDisk(jobData LongRunningJob) bool {
	readSuccess := initPersistenceLayer()
	if !readSuccess {
		return false
	}

	// Append the new record
	AllLongRunningJobs = append(AllLongRunningJobs, jobData)

	return WriteAllJobData()
}

func readJobData() ([]LongRunningJob, bool) {
	// Purge stale data
	AllLongRunningJobs = AllLongRunningJobs[:0]

	if !initPersistenceLayerCore() {
		genericCore.WriteToLog("Could not initialize persistence layer")
		return AllLongRunningJobs, false
	}

	fileInfo, err := os.Stat(jobsFile)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error running \"stat\" on cluster-director-mcp job history file %s : %v", jobsFile, err))
		return AllLongRunningJobs, false
	}

	// Empty file?
	if fileInfo.Size() == 0 {
		genericCore.WriteToLog(fmt.Sprintf("cluster-director-mcp job history file exists but is empty : %s", jobsFile))
		return AllLongRunningJobs, true
	}

	// Read JSON
	data, err := os.ReadFile(jobsFile)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error reading cluster-director-mcp job history file %s : %v", jobsFile, err))
		return AllLongRunningJobs, false
	}

	// Parse JSON
	err = json.Unmarshal(data, &AllLongRunningJobs)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error parsing cluster-director-mcp job history JSON: %v", err))
		return AllLongRunningJobs, false
	}

	return AllLongRunningJobs, true
}

func GetUniqueJobID() (int, bool) {

	//_, readSuccess := ReadJobData()
	readSuccess := initPersistenceLayer()
	if !readSuccess {
		genericCore.WriteToLog("Could not initialize persistence layer")
		return -1, false
	}

	// There are no jobs, this is the first job
	if len(AllLongRunningJobs) == 0 {
		genericCore.WriteToLog("Persistence layer did not read any running jobs")
		return 1, true
	}

	var maxCDMcpJobId int = 1
	for i := 1; i < len(AllLongRunningJobs); i++ {
		// 4. Update max if the current item is greater
		if AllLongRunningJobs[i].CDMcpJobId > maxCDMcpJobId {
			maxCDMcpJobId = AllLongRunningJobs[i].CDMcpJobId
		}
	}

	genericCore.WriteToLog(fmt.Sprintf("Returning unique job Id : %d \n", (maxCDMcpJobId + 1)))
	return int(maxCDMcpJobId + 1), true
}

func GetMostRecentJob(projectId string) (*LongRunningJob, bool, string) {
	var retObj *LongRunningJob = nil

	genericCore.WriteToLog("Getting recent jobs for project " + projectId)

	// Read Job data
	readSuccess := initPersistenceLayer()
	if !readSuccess {
		genericCore.WriteToLog("Could not read job data from persistence layer. Did the user delete ~/.cluster-director-mcp ?")
		return nil, false, "Could not read job data from persistence layer. Did you delete ~/.cluster-director-mcp ?"
	}

	// There are no jobs
	if len(AllLongRunningJobs) == 0 {
		genericCore.WriteToLog("Persistence layer did not read any running jobs from disk")
		return nil, true, "Persistence layer did not read any running jobs from disk!"
	}

	// There are no jobs
	if len(AllLongRunningJobs) == 1 {
		return &AllLongRunningJobs[0], true, ""
	}

	genericCore.WriteToLog(fmt.Sprintf("Read %d jobs from persistence layer", len(AllLongRunningJobs)))

	// Compute time window
	startOfTimeWindow := time.Now().Add(-JOB_EXPIRY_TIME_WINDOW)

	for i := 0; i < len(AllLongRunningJobs); i++ {
		genericCore.WriteToLog(fmt.Sprintf("Processing Cluster Director MCP JobId : %d \n", AllLongRunningJobs[i].CDMcpJobId))

		if AllLongRunningJobs[i].ProjectId != projectId {
			continue
		}

		// If this job was not submitted within the time window ignore it
		if startOfTimeWindow.After(AllLongRunningJobs[i].StartTime) {
			genericCore.WriteToLog(fmt.Sprintf("Ignoring Cluster Director MCP JobId %d outside of time window", AllLongRunningJobs[i].CDMcpJobId))
			continue
		}

		// Initialize retObj
		if retObj == nil {
			retObj = &AllLongRunningJobs[i]
			continue
		}

		// check if job is newer than job in retJob
		if AllLongRunningJobs[i].StartTime.After(retObj.StartTime) {
			retObj = &AllLongRunningJobs[i]
		}
	}

	if retObj != nil {
		genericCore.WriteToLog(fmt.Sprintf("Returning Cluster Director JobId : %d ", retObj.CDMcpJobId))
	} else {
		genericCore.WriteToLog("Could not find a job in \"recent\" time window")
	}

	return retObj, true, ""
}

func GetMostRecentRunningOrCompletedJob(projectId string) (*LongRunningJob, bool, string) {
	// First try to get the latest "running" jobs
	recentRunningOrCompletedJobs, success, mesg := GetALLRecentRunningJobs(projectId, false)
	if !success {
		genericCore.WriteToLog("Could not get recently running jobs")
		return nil, false, mesg
	}

	// If there were no RUNNING jobs, so fetch the most recent jobs that have completed
	if len(recentRunningOrCompletedJobs) == 0 {
		genericCore.WriteToLog("There were no recently running jobs, ignoring running status and getting most recent job")
		recentRunningOrCompletedJobs, _, _ = GetALLRecentRunningJobs(projectId, true)
	}

	if len(recentRunningOrCompletedJobs) == 1 {
		return &AllLongRunningJobs[recentRunningOrCompletedJobs[0]], true, ""
	}

	mostRecentJobIndex := 0
	for _, v := range recentRunningOrCompletedJobs {
		if AllLongRunningJobs[v].StartTime.After(AllLongRunningJobs[mostRecentJobIndex].StartTime) {
			mostRecentJobIndex = v
		}
	}

	return &AllLongRunningJobs[mostRecentJobIndex], true, ""
}

// Retun values:
// []int: Array of jobs marked running
// bool: Operation to probe job status was successful
// mesg: Mesg if Operation to probe job status was NOT successful
func GetAllLongRunningJobsInCluster(projectId string, clusterName string) ([]int, bool, string) {
	var allRecentRunningJobIndices = []int{}

	if !persistenceLayerReady {
		success := initPersistenceLayer()
		if !success {
			genericCore.WriteToLog("Could initialize persistence layer")
			return nil, false, "Could initialize persistence layer"
		}
	}

	// Filter for cluster name
	for i, _ := range AllLongRunningJobs {
		if AllLongRunningJobs[i].ClusterName == clusterName && AllLongRunningJobs[i].JobStatus == Running {
			allRecentRunningJobIndices = append(allRecentRunningJobIndices, i)
		}
	}

	return allRecentRunningJobIndices, true, ""
}

func GetALLRecentRunningJobs(projectId string, ignoreRunningStatus bool) ([]int, bool, string) {
	var allRecentRunningJobIndices []int

	//_, readSuccess := ReadJobData()
	readSuccess := initPersistenceLayer()
	if !readSuccess {
		genericCore.WriteToLog("Could initialize persistence layer")
		return allRecentRunningJobIndices, false, "Could not read job data from persistence layer. Did you delete ~/.cluster-director-mcp ?"
	}

	// There are no jobs
	if len(AllLongRunningJobs) == 0 {
		genericCore.WriteToLog("There were no jobs recorded in persistence layer!")
		return allRecentRunningJobIndices, false, "There were no jobs recorded in persistence layer!"
	}

	// Compute time window
	startOfTimeWindow := time.Now().Add(-JOB_EXPIRY_TIME_WINDOW)

	for i := 0; i < len(AllLongRunningJobs); i++ {
		if AllLongRunningJobs[i].ProjectId != projectId {
			continue
		}
		if !ignoreRunningStatus {
			if AllLongRunningJobs[i].JobStatus != Running {
				continue
			}
		}
		// If this job was not submitted within the time window ignore it
		if startOfTimeWindow.After(AllLongRunningJobs[i].StartTime) {
			continue
		}

		allRecentRunningJobIndices = append(allRecentRunningJobIndices, i)
	}

	return allRecentRunningJobIndices, true, ""
}

func GetJobIndexForCDMcpJobId(CDMcpJobId int) (int, bool) {
	for i := range AllLongRunningJobs {
		if AllLongRunningJobs[i].CDMcpJobId == CDMcpJobId {
			return i, true
		}
	}

	return -1, false
}

const CDMCP_REMOTE_ROOT_DIR = "~/cluster-director-mcp/"
const CDMCP_SHELL_SCRIPT_NAME = "cluster-director-mcp_run.sh"

const CDMCP_FULL_LOG = "log.cluster-director-mcp_test"
const CDMCP_SUMMARY_LOG = "cluster-director-mcp.summary.log"

// Note: This function creates a new job structure but does not append it to
// AllLongRunningJobs or write job data to disk
func GetNewJob(clusterName string,
	loginNodeName string,
	zone string,
	jobT LONG_RUNNING_OPERATION,
	machineType string,
	partitionName string,
	projectId string) (LongRunningJob, bool) {

	var newJob LongRunningJob
	var success bool
	newJob.CDMcpJobId, success = GetUniqueJobID()
	if !success {
		return newJob, false
	}

	newJob.SlurmJobId = UNDEFINED_VAL
	newJob.ProjectId = projectId
	newJob.ClusterName = clusterName
	newJob.LoginNodeName = loginNodeName
	newJob.Zone = zone
	newJob.StartTime = time.Now()

	newJob.JobType = jobT
	newJob.machineType = machineType
	newJob.partitionName = partitionName
	newJob.JobStatus = Running
	newJob.JobExecutionResult = JOB_EXEC_RESULT_DONT_KNOW
	newJob.JobExecutionResultString = ""

	newJob.RunDir = fmt.Sprintf(CDMCP_REMOTE_ROOT_DIR+"CDMcpJobId.%d", newJob.CDMcpJobId)
	newJob.RunScriptName = newJob.RunDir + "/" + CDMCP_SHELL_SCRIPT_NAME

	newJob.SummaryLogFilePath = newJob.RunDir + "/" + CDMCP_SUMMARY_LOG
	newJob.FullLogFilePath = newJob.RunDir + "/" + CDMCP_FULL_LOG

	return newJob, true
}
