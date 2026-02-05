# Cluster Director MCP Server and Gemini CLI Extension

Interact with Cluster Director in natural language to use, monitor, maintain and benchmark your Clusters.


## MCP Context

We install MCP servers as part of this software stallation, they are:
1. cluster-director-mcp server: Agentic AI-Assistant that can execute tools (listed in MCP Tools section) on behalf of the user.

## Installation and Running Cluster Director MCP

Cluster Director MCP Server is intended to be used on Google [Cloud Shell](https://docs.cloud.google.com/shell/docs/launching-cloud-shell) as a Gemini CLI extension.


1. Request the following IAM roles from the owner of your GCP project
   roles/compute.osLogin
   roles/iam.serviceAccountUser
   roles/compute.instanceAdmin.v1
   roles/iap.tunnelResourceAccessor
   
2. git clone https://github.com/GoogleCloudPlatform/cluster-director-mcp.git

3. Run gemini-cli with the necessary cluster-director-mcp extensions installed 

```sh
cd cluster-director-mcp; ./run.sh
```

## MCP Tools that are part of cluster-director-mcp server

- `check_job_status`: Shows the jobs running in cluster created using Cluster Director.
- `check_maintenance`: Checks for maintenance events for ALL the compute (GPU) nodes inthe cluster.
- `get_cluster`: Describe a cluster, i.e the type of compute nodes and storage provisioned.
- `list_clusters`: List clusters created using Cluster Director.
- `list_partition_info`: Shows information on a slurm partition in a cluster created using Cluster Director.
- `run_dcgm_test`: Runs DCGM tests on the cluster's GPU nodes to verify cluster health.
- `run_nccl_test`: Runs NCCL tests on the cluster's GPU nodes to verify cluster health.
- `show_cluster_software_version_info`: Show the software versions for ALL the compute (GPU) nodes in the cluster.
- `show_cluster_state`: Shows the state of the compute nodes in the cluster (idle, running jobs ..etc) created in Cluster Director.
- `show_job_state`: Shows the jobs running in cluster created using Cluster Director.
- `show_recent_jobs`: Shows the recent jobs that were run on the of cluster.





