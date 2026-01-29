### Cluster Director MCP Extension for Gemini CLI 

#### **Core Philosophy**

You are an expert-level AI Agent and Engineer specializing in GKE and Cluster Director Slurm clusters. Your goal is to answer questions on GKE and Slurm clusters and run the supported tools on behalf of the user. Before any action, you must announce the current workflow asnd phase. Do not try to diagnose solutions to errors reported from a tool call, let the user determine the next steps.

---

#### **Context & Rules**

This section configures your core behavior, ensuring you always use the best Cluster Toolkit documentation.

# Rule 1: For ALL Cluster Director questions, use the documentation in this guide.

[[calls]]
match = "For using clusters like checking job status, getting cluster info, list clusters, list partition info, run dcgm test, run nccl test, show cluster state and show recent jobs"
tool = "cluster-director-gke-ai"
args = []

## Guiding Principles

*   **Prefer Native Tools:** Always prefer to use the tools provided by this extension (e.g., `list_clusters`, `get_cluster`) instead of shelling out to `gcloud` for the same functionality. This ensures better-structured data and more reliable execution.
*   **Clarify Ambiguity:** Do not guess or assume values for required parameters like cluster names or locations. If the user's request is ambiguous, ask clarifying questions to confirm the exact resource they intend to interact with.
*   **Use Defaults:** If a `projectId` is not specified by the user, you can use the default value configured in the environment.

Choose a consumption option
This document explains the consumption options that you can select for getting and using compute resources for your clusters in Cluster Director. For each partition that you want to create in your cluster, choose the option that best fits your workload, its duration, and your cost needs.

Each consumption option specifies the following:

How you access compute resources to create virtual machine (VM) instances in your cluster.

The underlying provisioning model, which determines the obtainability, lifespan, and pricing of your VMs.

Comparison of consumption options
The following table summarizes the key differences between the consumption options:

Consumption option	Future reservations for blocks of capacity	Future reservations for up to 90 days (in calendar mode)	Flex-start	Spot	On-demand
Supported machines	A4X, A4, A3 Ultra, and A3 Mega	A4 and A3 Ultra	A4, A3 Ultra, and A3 Mega	A4, A3 Ultra, A3 Mega, and N2	N2
Lifespan	Unlimited	90 days maximum	7 days maximum	Unlimited (but subject to preemption)	Unlimited
Preemptible					
Capacity assurance	Very high. If Google Cloud approves your reservation request, then you have very high assurance that Compute Engine provisions your requested capacity.	Very high. If Google Cloud approves your reservation request, then you have very high assurance that Compute Engine provisions your requested capacity.	Best-effort. Compute Engine makes best-effort attempts to schedule the provisioning of your requested capacity.	Best-effort. Compute Engine makes best-effort attempts to provision your requested capacity.	Best-effort. Compute Engine makes best-effort attempts to provision your requested capacity.
Quota	Quota is automatically increased before capacity is delivered.	No quota is charged.	Preemptible quota is charged.	Preemptible quota is charged.	Standard quota is charged.
Pricing	
Discounted (up to 53%). See the pricing for accelerator-optimized VMs. If you reserve resources for a year or longer, then you must purchase and attach a resource-based commitment to your reserved resources.
You're charged for the reservation period. See reservations billing.
Discounted (up to 53%). See Dynamic Workload Scheduler pricing.
You're charged for the reservation period. See reservations billing.
Discounted (up to 53%). See Dynamic Workload Scheduler pricing.
You pay as you go (PAYG).
Deeply discounted (up to 91%). See Spot VMs pricing.
You pay as you go (PAYG).
Standard pricing. See the pricing for general-purpose VMs.
You pay as you go (PAYG).
Resource allocation	Dense. Resources are physically close to each other to minimize network hops and optimize for the lowest latency.	Dense. Resources are physically close to each other to minimize network hops and optimize for the lowest latency.	Best-effort. Resources are closely placed to each other on a best-effort basis.	Best-effort. Resources are closely placed to each other on a best-effort basis.	Best-effort. Resources are closely placed to each other on a best-effort basis.
Provisioning model	Reservation-bound	Reservation-bound	Flex-start	Spot	Standard
Creation prerequisites	
To create clusters, you must do the following:

Reserve capacity by contacting your account team.
At your chosen date and time, you can use the reserved capacity to create VMs in your cluster.
To create clusters, you must do the following:

Create a future reservation in calendar mode.
At your chosen date and time, you can use the reserved capacity to create VMs in your cluster.
If your requested capacity becomes available within your specified timeframe, Cluster Director creates the VMs. Otherwise, you encounter errors.	If resources are available, then you can immediately create VMs.	If resources are available, then you can immediately create VMs.
Choose a consumption option
Use the following flowchart to choose the consumption option that best fits the type of partition for your cluster:

A flowchart with the consumption options that are available in Cluster Director.

The questions in the preceding diagram are the following:

Do you want high assurance for GPU VMs?

Yes: Go to question 2

No: Go to question 4.

Do you need capacity for more than 90 days?

Yes: See Use future reservations for blocks of capacity.

No: Go to question 3.

Do you want reserved capacity?

Yes: See Use future reservations in calendar mode.

No: Go to question 4.

Is your workload fault-tolerant?

Yes: See Use Spot.

No: Go to question 5.

Do you want to obtain GPU VMs?

Yes: See Use Flex-start.

No: See Use On-demand.

Use future reservations for blocks of capacity
To run long-running, large-scale distributed workloads that require densely allocated resources, you can request to reserve compute resources for a specific time in the future. If your request is approved, then you have exclusive access to your reserved resources for that period of time, and you can use the resources to create clusters. At the end of the reservation period, Compute Engine does the following:

Compute Engine deletes the reservation.

Based on the termination action that you specify when creating your cluster, Compute Engine stops or deletes any VMs that use the reservation.

Ideal workloads for future reservations for blocks of capacity
Future reservations for blocks of capacity are ideal for the following workloads:

Pre-training foundation models

Multi-host foundation model inference

Key characteristics of future reservations for blocks of capacity
Future reservations for blocks of capacity have the following characteristics:

You can reserve A4X, A4, A3 Ultra, and A3 Mega machine types. Machines are densely allocated to minimize network latency.

You can reserve as many VMs as you want for up to a year. For any VMs you want to reserve for a year or longer, you need to purchase and attach a resource-based commitment to your reserved resources. Then, you can use the reserved resources to create and run VMs until the end of the reservation period. to your reserved resources.

You use the reservation-bound provisioning model, which has the following benefits:

You have a higher chance of obtaining GPUs.

In addition to the commitment attached to your VMs, you get a discount up to 53% for vCPUs, memory, and GPUs.

How to use future reservations for blocks of capacity
To use future reservations for blocks of capacity to create clusters, you must complete the following steps:

Request to reserve capacity. Contact your account team and specify the resources to reserve. Based on availability, Google creates a draft reservation request for you. If it looks correct, then you can submit it. Google Cloud immediately approves your reservation request.

For instructions, see Reserve capacity through your account team.

Consume reserved resources. At the start of your chosen reservation period, you can use the reservation to create VMs in one or more of your cluster partitions.

For instructions, see one of the following:

Create an AI-optimized cluster based on a template

Create a custom cluster

Use future reservations in calendar mode
To run short-running distributed workloads that require densely allocated resources, you can request to reserve compute resources for up to 90 days. If your request is approved, then you have exclusive access to your reserved resources for that time, and you can use the resources to create clusters. At the end of the reservation period, Compute Engine does the following:

Compute Engine deletes the reservation.

Based on the termination action that you specify when creating your cluster, Compute Engine stops or deletes any VMs that use the reservation.

Ideal workloads for future reservations in calendar mode
Future reservations in calendar mode are ideal for the following workloads:

Model pre-training

Model fine-tuning

Simulations

Inference

Key characteristics of future reservations in calendar mode
Future reservations in calendar mode have the following characteristics:

You can reserve A4 or A3 Ultra machine types. These machines are densely allocated to minimize network latency.

You can view the future availability of resources, and then reserve up to 80 VMs for up to 90 days in the future. Then, you can use the reserved resources to create VMs until the end of the reservation period.

You use the reservation-bound provisioning model, which has the following benefits:

You have a higher chance of obtaining GPUs.

You get a discount up to 53% for vCPUs, memory, and GPUs.

How to use future reservations in calendar mode
To use future reservations in calendar mode to clusters, you must complete the following steps:

View resources availability. You can view the future availability of the resources that you want to reserve. When you create a reservation request, you can specify the number, type, and reservation duration for the resources that you confirmed as available. This action increases the chances that Google Cloud approves your request.

For instructions, see View resource future availability.

Reserve capacity. You create a reservation request for a future date and time. Google Cloud approves the reservation request within two minutes. If approved, then Compute Engine reserves the capacity for you. At your chosen delivery date, you can use the reserved resources to create clusters.

For instructions, see Create a reservation request for GPU VMs or TPUs.

Consume reserved resources. At the start of your chosen reservation period, you can use the reservation to create VMs in one or more of your cluster partitions.

For instructions, see one of the following:

Create an AI-optimized cluster based on a template

Create a custom cluster

Use Flex-start
To run short-duration workloads that require densely allocated resources, you can request compute resources for up to seven days by using Flex-start. Whenever resources are available, Compute Engine creates your requested number of VMs. The Flex-start VMs run until you delete them, or until Compute Engine deletes the VMs at the end of their run duration.

Ideal workloads for Flex-start
Flex-start is ideal for workloads that can start at any time, such as the following:

Small model pre-training

Model fine-tuning

Simulations

Batch inference

Key characteristics of Flex-start
Flex-start has the following characteristics:

You can request A4, A3 Ultra, and A3 Mega VMs. Dense allocation depends on resource availability.

You use the flex-start provisioning model, which has the following benefits:

You have a higher chance of obtaining GPUs.

You get a discount up to 53% for vCPUs, memory, and GPUs.

How to use Flex-start
To use Flex-start to create VMs in one or more of your cluster partitions, use one of the following methods:

Create an AI-optimized cluster based on a template

Create a custom cluster

Use Spot
To run fault-tolerant workloads, you can obtain compute resources immediately based on availability. You get resources at the lowest price possible. However, Compute Engine can preempt VMs at any time to reclaim capacity.

Ideal workloads for Spot
Spot is ideal for workloads where interruptions are acceptable, such as the following:

Batch processing

High performance computing (HPC)

Continuous integration and continuous deployment (CI/CD)

Data analytics

Media encoding

Online inference

Key characteristics of Spot
Spot has the following characteristics:

You can create A4, A3 Ultra, A3 Mega, and N2 VMs. Dense allocation depends on resource availability.

You can immediately create clusters. The VMs in the cluster run until you stop or delete them, or until Compute Engine preempts the VMs to reclaim capacity.

You use the spot provisioning model, which has the following benefits:

You have a higher chance of obtaining GPUs.

You get a discount of up to 91% off for many machine types, GPUs, TPUs, and Local SSDs

How to use Spot
To use Spot to create VMs in one or more of your cluster partitions, use one of the following methods:

Create an AI-optimized cluster based on a template

Create a custom cluster

Use On-demand
For cluster components that don't require GPU acceleration, such as login nodes, or for running CPU-bound computational tasks like HPC workloads, you can get resources immediately based on availability. You get resources at the standard pricing.

Ideal workloads for On-demand
On-demand is ideal for workloads that don't require GPU-acceleration, such as the following:

Login nodes

CPU-bound computational tasks

General-purpose HPC

Key characteristics of On-demand
On-demand has the following characteristics:

You can create N2 VMs. Dense allocation depends on resource availability.

You can immediately create clusters. The VMs in the cluster run until you stop or delete them.

You use the standard provisioning model, which is the default model.

How to use On-demand
To use On-demand to create VMs in one or more of your cluster partitions, use one of the following methods:

Create an AI-optimized cluster based on a template

Create a custom cluster
Create an AI-optimized cluster based on a template
This document explains how to create a cluster in Cluster Director by using a pre-configured template that is optimized for running artificial intelligence (AI) and machine learning (ML) training workloads.

This template specifies optimized compute, networking, and storage resources that are designed for high reliability and resiliency and are pre-configured for most AI and ML training workloads. When creating the cluster, you can optionally edit one or more settings to meet the needs of your workloads. This approach helps you to quickly provision a cluster while maintaining the flexibility to make changes if your workload needs it.

To create a fully customized cluster from scratch, see Create a custom cluster.

Limitations
When you create a cluster in Cluster Director, the following limitations apply:

Regional scope: clusters are regional resources. You can only create or use compute and storage resources that exist within the same region as your cluster.

Compute resource configuration per nodeset: you can only assign one compute resource configuration for each nodeset that you want to create in your cluster.

Storage classes for new Cloud Storage buckets: if you plan to create one or more buckets when creating a cluster, then you can only specify the standard storage class. If you want to use an automatically-assigned storage class, then you must update the bucket after creating the cluster.

Requirements for N2 machine types for the login node: in the login node, you can only specify N2 standard machine types with 32 vCPUs or fewer.

Before you begin
Before you create a cluster in Cluster Director, do the following:

Choose consumption options. If you haven't already, then you must choose the consumption options for the virtual machine (VM) instances that you want to use in each partition for your cluster. Each consumption option determines the availability, obtainability, and pricing for your VMs.

To learn more, see Choose a consumption option.

Obtain capacity and quota. Based on your chosen consumption option, review the quota requirements for the VMs that you want to create in the cluster. If you lack sufficient quota, then creating your cluster fails.

To learn more, see Capacity and quota overview.

Verify usable reservations If you want to create your cluster by using one or more reservations, then verify that the reservations have enough available resources to create your chosen number of VMs in the cluster. Otherwise, skip this step.

To learn more, see Consumable VMs in a reservation.

Verify existing resource requirements. If you plan to use existing storage or networking resources in your cluster, then you must verify that those resources meet the following configuration requirements. Otherwise, skip this step.

Existing Filestore instances: to use one or more existing Filestore instances, the following requirements apply:

The instances and the cluster must use the exact same network.

The instances can only specify regional or zonal service tiers.

To verify the network and service tier that a Filestore instance uses, see Get instance information.

Existing VPC network: to use an existing Virtual Private Cloud (VPC) network for your cluster, you must configure the network as follows:

You must enable Private Google Access configuration.

Your must configure the firewall rules in the network to allow SSH ingress and internal access.

Required roles
To get the permissions that you need to create a cluster based on a template, ask your administrator to grant you the Hypercompute Cluster Editor (roles/hypercomputecluster.editor) IAM role on the project. For more information about granting roles, see Manage access to projects, folders, and organizations.

This predefined role contains the permissions required to create a cluster based on a template. To see the exact permissions that are required, expand the Required permissions section:

Required permissions
You might also be able to get these permissions with custom roles or other predefined roles.

Create an AI-optimized cluster based on a template
You can create a cluster by using a template that has the following pre-configured settings:

A login node with one N2 virtual machine (VM) instance

A compute nodeset with four A3 Ultra VMs

A new Basic SSD Filestore instance of 5 TB

A new Google Cloud Managed Lustre of 36 TiB

When creating your cluster, you can either use the pre-configured settings in the template, or edit one or more settings based on the demands of your workloads.

To create an AI-optimized cluster based on a template, complete the following steps:

Configure compute resource configurations

Configure network

Configure storage resources

Configure the Slurm environment

Configure compute resource configurations
To configure compute resource configurations when creating a cluster, complete the following steps:

In the Google Cloud console, go to the Cluster Director page.

Go to Cluster Director

Click add Create cluster.

In the dialog that appears, click Reference architecture. The Create a cluster page appears.

Click AI/ML training cluster.

In the Cluster name field, enter a name for your cluster. The name can contain up to 10 characters, and it can only use numbers or lowercase letters (a-z).

To add information to the pre-configured compute resource configuration, or edit the number and type of VMs that the configurations specifies, do the following:

In the Compute section, click edit Edit resource configuration. The Add resource configuration pane appears.

Optional: To change the compute resource configuration name, in the Name field, enter a new name.

Optional: To change the number and type of VMs that your cluster uses, in the Machine configuration section, follow the prompts to update the compute resources.

In the Consumption options section, specify the consumption option that you want to use to obtain resources:

To create VMs by using a reservation, do the following:

Click the Use reservation tab.

Click Select reservation. The Choose a reservation pane appears.

Select the reservation that you want to use. Then, click Choose. This action automatically sets the Region and Zone of your compute resources.

To create Flex-start VMs, do the following:

Click the Flex start tab.

In the Time limit for the VM section, specify the run duration for the VMs. The value must be between 10 minutes and 7 days.

In the Location section, select the region where you want to create Flex-start VMs. The Google Cloud console automatically filters the available regions to only show only those regions that support Flex-start VMs for your selected machine type.

To create Spot VMs, do the following:

Click the Use spot tab.

In the On VM termination list, select one of the following options:

To delete Spot VMs on preemption, select Delete.

To stop Spot VMs on preemption, select Stop.

In the Location section, select the Region and Zone where you want to create Spot VMs. The Google Cloud console automatically filters the available regions to only show only those regions that support Spot VMs for your selected machine type.

Click Done.

Optional: To create additional compute resource configurations for a partition, click the add Add resource configuration, and then follow the prompts to specify the compute resources.

Click Continue. The Networking pane appears.

Configure network
To configure the network that your cluster uses, complete the following steps:

In the Choose new or existing network section, do one of the following:

Recommended: To let Cluster Director automatically create a network with the required firewall rules, select Create network.

To use an existing network, do the following:

Select Select existing network.

In the Select VPC network list, select an existing network.

In the Select subnetwork list, select an existing subnetwork.

Click Continue. The Storage pane appears.

Configure storage resources
To configure the storage resources that your cluster uses, complete the following steps:

Optional: To edit a storage resource, click the edit Edit storage plan, and then follow the prompts to update the configuration of the storage resource.

Optional: To add storage resources to your cluster, click add Add storage configuration, and then follow the prompts to specify the configuration for the storage resources.

Click Continue. The Orchestration pane appears.

Configure the Slurm environment
To configure the Slurm environment in your cluster, complete the following steps:

Optional: To edit the number and type of VMs that the login node uses, expand the Login node section, and then follow the prompts to update the compute resources.

Optional: To edit partitions of your cluster to organize your compute resources, expand the Partitions section, and then do one of the following:

To add a partition, click Add partition, and then do the following:

In the Partition name field, enter a name for the partition.

To edit a nodeset, click Toggle nodeset. Otherwise, to add a nodeset, click Add nodeset.

In the Nodeset name field, enter a name for your nodeset.

In the Resource configuration field, select a compute resource configuration that you created in the previous steps.

In the Static node count field, enter the number of VMs that must always be running in the cluster.

In the Dynamic node count field, enter a maximum number of VMs that Cluster Director can increase the cluster to during increases in traffic.

Important: If you create VMs in the nodeset by using a reservation, especially a shared reservation, then verify that the reservation has enough resources available to create your specified maximum number of VMs. Other workloads that use the same reservation might fully use it and, thus, Cluster Director might be unable to create more VMs in your nodeset.
In the Boot disk type list and Boot disk size field, enter the type and size of the boot disk for the VMs to use.

Click Done.

To remove a partition, click delete Delete partition.

Optional: To add prolog or epilog scripts to your Slurm environment, do the following:

Expand the Advanced orchestration settings section.

In the Scripts section, follow the prompts to add scripts.

Click Create.

Creating the cluster can take some time to complete. If your requested resources are unavailable, then Cluster Director reattempts the creation request until resources become available. When all your requested resources become available, the cluster state changes to Ready, and the cluster creation request completes.

What's next?
View clusters

Connect to a cluster

Modify a clusterCreate a custom cluster from scratch
This document explains how to create a cluster in Cluster Director where you fully customize the compute, networking, and storage resources for your specific artificial intelligence (AI), machine learning (ML), or high performance computing (HPC) workloads.

This process lets you design a fault-tolerant and highly scalable Slurm environment from scratch, helping to ensure that your cluster meets the needs of your workloads.

To create a cluster based on a template that is optimized for running AI and ML workloads, see Create an AI-optimized cluster based on a template.

Limitations
When you create a cluster in Cluster Director, the following limitations apply:

Regional scope: clusters are regional resources. You can only create or use compute and storage resources that exist within the same region as your cluster.

Compute resource configuration per nodeset: you can only assign one compute resource configuration for each nodeset that you want to create in your cluster.

Storage classes for new Cloud Storage buckets: if you plan to create one or more buckets when creating a cluster, then you can only specify the standard storage class. If you want to use an automatically-assigned storage class, then you must update the bucket after creating the cluster.

Requirements for N2 machine types for the login node: in the login node, you can only specify N2 standard machine types with 32 vCPUs or fewer.

Before you begin
Before you create a cluster in Cluster Director, do the following:

Choose consumption options. If you haven't already, then you must choose the consumption options for the virtual machine (VM) instances that you want to use in each partition for your cluster. Each consumption option determines the availability, obtainability, and pricing for your VMs.

To learn more, see Choose a consumption option.

Obtain capacity and quota. Based on your chosen consumption option, review the quota requirements for the VMs that you want to create in the cluster. If you lack sufficient quota, then creating your cluster fails.

To learn more, see Capacity and quota overview.

Verify usable reservations. If you want to create your cluster by using one or more reservations, then verify that the reservations have enough available resources to create your chosen number of VMs in the cluster. Otherwise, skip this step.

To learn more, see Consumable VMs in a reservation.

Verify existing resource requirements. If you plan to use existing storage or networking resources in your cluster, then you must verify that those resources meet the following configuration requirements. Otherwise, skip this step.

Existing Filestore instances: to use one or more existing Filestore instances, the following requirements apply:

The instances and the cluster must use the exact same network.

The instances can only specify regional or zonal service tiers.

To verify the network and service tier that a Filestore instance uses, see Get instance information.

Existing VPC network: to use an existing Virtual Private Cloud (VPC) network for your cluster, you must configure the network as follows:

You must enable Private Google Access configuration.

Your must configure the firewall rules in the network to allow SSH ingress and internal access.

Authenticate.

Select the tab for how you plan to use the samples on this page:

Console
gcloud
REST
In one of the following development environments, set up the gcloud CLI:

Cloud Shell: to use an online terminal with the gcloud CLI already set up, activate Cloud Shell.

Activate Cloud Shell on this page

At the bottom of this page, a Cloud Shell session starts and displays a command-line prompt. It can take a few seconds for the session to initialize.

Local shell: to use a local development environment, install and initialize the gcloud CLI.

If you're using an external identity provider (IdP), you must first sign in to the gcloud CLI with your federated identity.

Note: If you installed the gcloud CLI previously, make sure you have the latest version by running gcloud components update.
Required roles
To get the permissions that you need to create a custom cluster from scratch, ask your administrator to grant you the Hypercompute Cluster Editor (roles/hypercomputecluster.editor) IAM role on the project. For more information about granting roles, see Manage access to projects, folders, and organizations.

This predefined role contains the permissions required to create a custom cluster from scratch. To see the exact permissions that are required, expand the Required permissions section:

Required permissions
You might also be able to get these permissions with custom roles or other predefined roles.

Create a custom cluster from scratch
To create a custom cluster from scratch, select one of the following options:

Console
gcloud
REST
To create a cluster from scratch, use the gcloud alpha cluster-director clusters create command.

Based on how you want to specify the cluster configuration, use one of the following methods:

Specify a configuration file: to create a cluster by specifying the cluster configuration in a JSON file, use the --config flag. To run the command, select one of the following options:

Bash
Powershell
cmd.exe


gcloud alpha cluster-director clusters create CLUSTER_NAME \
    --location=REGION \
    --config=CONFIGURATION_FILE
Replace the following:

CLUSTER_NAME: the name of the cluster. The name can contain up to 10 characters, and it can only use numbers or lowercase letters.

REGION: the region where to create your cluster.

CONFIGURATION_FILE: the path to the JSON file that contains the configuration details for the cluster. To review the configuration details that you can specify, review the request body for creating a cluster by using REST.

Specify cluster properties directly: to create a cluster by specifying each configuration property directly, use the following flags:

To specify a network, use one of the following flags:

To create a new network: --create-network

To use an existing network and subnetwork: --network and --subnet

To specify a Filestore instance, use one of the following flags:

To create a new instance: --create-filestores

To use an existing instance: --filestores

Optionally, to specify a Cloud Storage bucket, use one of the following flags:

To create a new bucket: --create-buckets

To use an existing bucket: --buckets

Optionally, to specify a Google Cloud Managed Lustre instance, use one of the following flags:

To create a new instance: --create-lustres

To use an existing instance: --lustres

To specify a compute resource configuration, use one of the following flags for each resource configuration that you want to create in the cluster.

To create VMs by using a reservation: --reserved-instances

To create Flex-start VMs: --dws-flex-instances

To create Spot VMs: --spot-instances

To create N2 on-demand VMs: --on-demand-instances

To specify the configuration for the login node, use the --slurm-login-node flag.

To specify the configuration for a compute nodeset, use the --slurm-node-sets flag. You can repeat this flag for each nodeset in the cluster.

To specify the cluster partitions, use the --slurm-partitions flag. You can repeat this flag for each partition in the cluster.

To specify the default partition for the cluster, use the --slurm-default-partition flag.

For example, assume that you want to create a cluster with one partition that uses reserved VMs, one partition that uses Spot VMs, a new Filestore instance, and a new network. To create the example cluster, select one of the following options:

Bash
Powershell
cmd.exe


gcloud alpha cluster-director clusters create CLUSTER_NAME \
    --location=REGION \
    --create-network=name=NETWORK_NAME \
    --create-filestores=name="locations/FILESTORE_INSTANCE_ZONE/instances/FILESTORE_INSTANCE_NAME",tier=TIER,capacityGb=CAPACITY,fileshare=SHARE_NAME,protocol=PROTOCOL \
    --reserved-instances=id=COMPUTE_RESOURCE_NAME_1,reservation="projects/RESERVATION_PROJECT_ID/zones/RESERVATION_ZONE/reservations/RESERVATION_NAME",machineType=RESERVATION_MACHINE_TYPE \
    --spot-instances=id=COMPUTE_RESOURCE_NAME_2,zone=SPOT_VMS_ZONE,machineType=SPOT_MACHINE_TYPE \
    --slurm-login-node=machineType=LOGIN_NODE_MACHINE_TYPE,zone=LOGIN_NODE_ZONE,count=LOGIN_NODES_COUNT \
    --slurm-node-sets=id=NODESET_NAME_1,computeId=COMPUTE_RESOURCE_NAME_1,staticNodeCount=NODESET_1_STATIC_COUNT,maxDynamicNodeCount=NODESET_1_MAX_DYNAMIC_COUNT \
    --slurm-node-sets=id=NODESET_NAME_2,computeId=COMPUTE_RESOURCE_NAME_2,staticNodeCount=NODESET_2_STATIC_COUNT,maxDynamicNodeCount=NODESET_2_MAX_DYNAMIC_COUNT \
    --slurm-partitions=id=PARTITION_NAME_1,nodesetIds=[NODESET_NAME_1] \
    --slurm-partitions=id=PARTITION_NAME_2,nodesetIds=[NODESET_NAME_2] \
    --slurm-default-partition=PARTITION_NAME_1
Replace the following:

CLUSTER_NAME: the name of the cluster. The name can contain up to 10 characters, and it can only use numbers or lowercase letters (a-z). Spaces or special characters aren't allowed.

REGION: the region where to create your cluster.

NETWORK_NAME: the name of the network that you want to create.

FILESTORE_INSTANCE_ZONE: the zone where you want to create your Filestore instance.

FILESTORE_INSTANCE_NAME: the name for your Filestore instance.

TIER: the type of service tier that you want to use for the instance and that Cluster Director supports. Specify one of the following values:

For the zonal tier: ZONAL

For the regional tier: REGIONAL

CAPACITY: the size, in GiB, that you want to allocate for the instance. The value must be between 1,024 GiB (1024) and 102,400 GiB (102400), and it must be in 256 GiB (256) increments.

SHARE_NAME: the name for the NFS file share that is served from the instance.

PROTOCOL: the system protocol for the instance. Specify one of the following values:

For NFSv3: NFSV3

For NFSv4.1: NFSV41

COMPUTE_RESOURCE_NAME_1 and COMPUTE_RESOURCE_NAME_2: the name of the two compute resource configurations.

RESERVATION_PROJECT_ID: the ID of the project where the reservation exists. If you want to use a reservation from a different project, then verify that your project is allowed to consume the reservation. For more information, see Allow and restrict projects from creating and modifying shared reservations.

RESERVATION_ZONE: the zone where the reservation exists.

RESERVATION_NAME: the name of the reservation that you want to use to create VMs.

RESERVATION_MACHINE_TYPE: the machine type that is specified in the reservation.

SPOT_VMS_ZONE: the zone where you want to create your Spot VMs. To review the regions and zones where the machine type that you want to use is available, see Available regions and zones.

SPOT_MACHINE_TYPE: the machine type to use for the Spot VMs. Specify one of the following machine types:

For an A4 machine type: a4-highgpu-8g

For an A3 Ultra machine type: a3-ultragpu-8g

For an A3 Mega machine type: a3-megagpu-8g

For an N2 machine type, see N2 machine series.

LOGIN_NODE_MACHINE_TYPE: the machine type that you want the VMs in the login nodeset to use. Specify an N2 standard machine type with 32 or fewer vCPUs.

LOGIN_NODE_ZONE: the zone where you want to create the VMs in the login nodeset.

LOGIN_NODES_COUNT: the number of VMs to use for the login nodeset.

NODESET_NAME_1 and NODESET_NAME_2: the name of the two nodesets.

NODESET_1_STATIC_COUNT and NODESET_2_STATIC_COUNT: the minimum number of VMs that must always be running in each nodeset.

NODESET_1_MAX_DYNAMIC_COUNT and NODESET_2_MAX_DYNAMIC_COUNT: the maximum number of VMs that Cluster Director can add to each nodeset during increases in traffic.

Important: If you create VMs in the nodeset by using a reservation, especially a shared reservation, then verify that the reservation has enough resources available to create your specified maximum number of VMs. Other workloads that use the same reservation might fully use it and, thus, Cluster Director might be unable to create more VMs in your nodeset.
PARTITION_NAME_1 and PARTITION_NAME_2: the name of the partitions for your cluster.

The output is similar to the following:


Create request issued for: [cluster000]
Waiting for operation [projects/example-project/locations/us-central1/operations/operation-1759856594716-640948b2f058e-f403bef9-1a08178a] to complete...working...
Creating the cluster can take some time to complete. If your requested resources are unavailable, then Cluster Director reattempts the creation request until resources become available. When all your requested resources become available, the cluster state changes to READY, the cluster creation request completes, and the output is similar to the following:


Created cluster [cluster000].
Cluster creation process overview
This document helps you understand the components that you need to configure before creating a cluster to meet the requirements of your workload.

Cluster creation process
Creating a cluster involves configuring compute, storage, and networking resources, as well as the Slurm orchestrator. The following process is to help you pick the configurations that best fits your workload needs:

Configure compute resources

Configure storage resources

Configure networking resources

Configure the Slurm orchestrator

Configure compute resources
You must choose the type and number of virtual machine (VM) instances that you want to create in your cluster. You must consider the following:

Machine series: you must choose the machines series and types that you want to create in your cluster. Each cluster partition can use a different machine series. Each virtual machine (VM) instance in your cluster serves as a node. For each type of node, we recommend the following machine series:

Compute nodes: based on your workload, use one of the following machine series:

Foundational model training and inference: A4X machine series

Large model training, fine-tuning, and inference: A4 or A3 Ultra machine series

Mainstream model inference, fine tuning, and serving inference: A3 Mega machine series

Controller nodes: use the N2 machine series.

Login nodes: use the N2 machine series with standard machine types, and 32 vCPUs or fewer.

Location: based on the locations where your chosen machine series are available, you must select the region and zone where you want to deploy your cluster. For more information, review the regions and zones where machine series are available.

Consumption option: you must choose the consumption option that you want to use to get and use compute resources. Each option affects the availability, obtainability, and pricing of a compute resource. For more information, see Choose a consumption option.

Configure storage resources
You must define the storage that your cluster uses:

Shared file system for /home directory: a Filestore instance is required for the shared /home directory in a Slurm cluster. When you create a cluster, you can do one of the following:

Let Cluster Director create a new Filestore instance with pre-configured settings.

Use an existing Filestore instance on the same network as your cluster.

Additional storage: for large-scale AI workloads that require more storage, you can add other solutions, such as Google Cloud Managed Lustre or Cloud Storage buckets.

Configure networking resources
You must define the networking that your cluster uses:

Create a new VPC network: if you choose this option, then Cluster Director creates the network on your behalf, and it automatically configures the necessary firewall rules to let you connect to your cluster.

Use an existing VPC network: if you want to use an existing network, then you must configure the network as follows:

You must enable Private Google Access for your network. For more information, see Private Google Access configuration.

Your must configure firewall rules to allow SSH ingress and internal access. For more information, see Use VPC firewall rules.

Configure the Slurm orchestrator
For the final step, you must configure the Slurm orchestrator. This process involves configuring the following components:

Login and controller nodes: you must define the machine series and boot disk options for the login and controller nodes. These nodes manage the cluster and let you connect to it.

Partitions and nodesets: you must define how you want to group your compute resources. Each partition can contain one or more nodesets, letting you create different pools of resources for different types of jobs. For each nodeset, you must also specify the minimum and maximum number of VMs that the nodeset can contain.

What's next?
Try out Cluster Director

Choose a consumption optionCreate a Slurm cluster
This quickstart guides you through creating and connecting to a Slurm cluster in Cluster Director by using an AI/ML training template. The cluster contains the following partitions and virtual machine (VM) instances:

For the login node, the cluster uses an N2 VM.

For the two compute nodes, the cluster uses A3 Mega Spot VMs.

This cluster configuration uses on-demand and spot resources, letting you create a cluster without requiring reserved capacity for a future date and time. After you connect to the cluster, you run sample jobs by using Slurm commands for job management.

Before you begin
In the Google Cloud console, on the project selector page, select or create a Google Cloud project.

Roles required to select or create a project

Note: If you don't plan to keep the resources that you create in this procedure, create a project instead of selecting an existing project. After you finish these steps, you can delete the project, removing all resources associated with the project.
Go to project selector

Verify that billing is enabled for your Google Cloud project.

Make sure that you have the following role or roles on the project: Cluster Director Editor, Compute OS Login, and IAP-secured Tunnel User

Check for the roles
Grant the roles
You must have sufficient quota for the resources used in this quickstart. For more information, see Allocation quotas.
Enable the Hypercompute Cluster API, Compute Engine API, and Filestore API:

Enable the Cluster Director, Compute Engine, and Filestore API
Costs
This quickstart uses the following billable Google Cloud resources:

Compute Engine: an N2 VM and two A3 Mega Spot VMs

Filestore: a Filestore instance

Google Cloud Managed Lustre: a Managed Lustre instance

To generate a cost estimate based on your projected usage, use the pricing calculator.

Create a Slurm cluster
To create a Slurm cluster, complete the following steps:

In the Google Cloud console, go to the Cluster Director page.

Go to Cluster Director

Click add Create a cluster.

In the dialog that appears, click Reference architecture. The Create cluster page appears.

Click AI/ML training cluster.

In the Cluster name field, enter cluster000.

In the Compute section, do the following:

To configure the compute resources for the first partition, complete the following steps:

Click edit Edit resource configuration. The Add resource configuration pane appears.

In the Name field, enter spot-configuration-1.

In the GPU type list, select NVIDIA H100 80GB mega.

In the Number of instances field, enter 1.

In the Consumption options section, click Use spot.

In the Location section, specify the following options:

For Region, select us-central1.

For Zone, select us-central1-a.

Click Done.

To configure the compute resources for the second partition, complete the following steps:

Click add Add resource configuration. The Add resource configuration pane appears.

In the Name field, enter spot-configuration-2.

Click the GPUs tab.

In the Consumption options section, click Use spot.

Click Done.

In the navigation menu, click Networking. The Networking pane appears.

In the Network name field, enter cluster-network-000.

In the navigation menu, click Orchestration. The Orchestration pane appears.

Expand the Partitions section. Then, do the following:

To edit the first partition, complete the following steps:

Click the part1 toggle.

In the Partition name field, enter spot1.

Click the nodeset1 toggle.

In the Edit nodeset card, do the following:

In the Static node count field, enter 1.

Click Done.

Click Done.

To edit the second partition, complete the following steps:

Click the part2 toggle.

In the Partition name field, enter spot2.

Click Done.

To create the cluster, click Create. The Clusters page appears.

Creating the cluster can take some time to complete. If your requested resources are unavailable, then Cluster Director reattempts the creation request until resources become available. When all your requested resources become available, the cluster state changes to Ready, and the cluster creation request completes. You can then proceed to the next section.

Connect to your cluster through SSH
When the state of your cluster changes to Ready, connect to the cluster through SSH by completing the following steps:

In the Name column, click cluster000. A page that gives the details of the cluster appears, and the Details tab is selected.

Click the Nodes tab.

In the Login nodes table, find the row that contains the cluster000-login-001 node. In that row, in the Connect column, click the SSH button. The SSH-in-browser window appears.

If prompted, then click Authorize. Connecting to your cluster can take some time to complete. When the terminal is ready, proceed to the next section.

Note: If you encounter errors when connecting to your cluster, then see Troubleshooting SSH errors.
Run sample jobs
In the SSH-in-browser window, complete the following steps:

To verify that Slurm is running, run the following command:



sinfo
To submit a test job that returns the hostname of the node, run the following command:



srun hostname
To submit a batch job that sleeps for 30 seconds, run the following command:



sbatch --wrap="sleep 30"
To check the status of jobs in the queue, run the following command:



squeue
To view accounting data for jobs, run the following command:



sacct
You've successfully created a Slurm cluster, connected to it, and run sample jobs!

Clean up
To avoid incurring charges to your Google Cloud account for the resources used on this page, follow these steps.

Delete your project
The easiest way to eliminate billing is to delete the project that you created for the tutorial.

To delete the project:

Caution: Deleting a project has the following effects:
Everything in the project is deleted. If you used an existing project for the tasks in this document, when you delete it, you also delete any other work you've done in the project.
Custom project IDs are lost. When you created this project, you might have created a custom project ID that you want to use in the future. To preserve the URLs that use the project ID, such as an appspot.com URL, delete selected resources inside the project instead of deleting the whole project.
If you plan to explore multiple architectures, tutorials, or quickstarts, reusing projects can help you avoid exceeding project quota limits.

In the Google Cloud console, go to the Manage resources page.
Go to Manage resources

In the project list, select the project that you want to delete, and then click Delete.
In the dialog, type the project ID, and then click Shut down to delete the project.
Delete your cluster
To delete the cluster, and its associated resources, that you created as part of this quickstart, complete the following steps:

On the page that contains the details of your cluster, click delete Delete.

In the dialog that appears, enter cluster000, and then click Delete to confirm.

What's next
Choose a consumption option
Get support
Where to get support
The following table summarizes the types of support that you might request for Cluster Director and where to get support for that request.

Type of support	Where to get support
Technical support	
If you purchase a Cloud Customer Care package, then you can contact technical support.

Alternatively, you can get support from the community.

For more information, see Get technical support in this document.

Product feedback (bugs, feature requests, general feedback)	See Product feedback in this document.
Documentation feedback (incorrect documentation, documentation requests, general feedback)	See Documentation feedback in this document.
Get technical support
The following sections summarize the options that you can use to get support for Cluster Director.

Get a Cloud Customer Care package
Google Cloud offers different support packages to meet different needs, such as 24/7 coverage, phone support, and access to a technical account manager. For more information, see Cloud Customer Care.

Provide feedback
The following sections summarize the options that you can use to provide direct feedback about Cluster Director and its documentation. For example, report issues, make suggestions, or provide general feedback.

Product feedback
You can use the following options to provide product feedback:

If you have a paid Cloud Customer Care package and are experiencing a production issue or blocker, contact Customer Care.
Send product feedback comments: From the beginning of any page in the Cluster Director documentation, click the Send feedback button. Then, select Product feedback.
The following sections list information that we recommend you provide when you file product feedback.

File bugs and defects
Example: Feature X is broken for Cluster Director.

For feedback about bugs and defects, we recommend that you provide the following information (if applicable):

What steps will reproduce the problem?
What is the expected output? What do you see instead?
What version of the product are you using? On what operating system?
Provide any additional information.
File feature requests
Example: Cluster Director should add feature X.

For feedback about feature requests, we recommend that you provide the following information (if applicable):

What is the expected behavior of the feature? (Be specific!)
If relevant, why are current approaches or workarounds insufficient?
If relevant, what new use cases will this feature enable?
Documentation feedback
Example: Documentation for X is out of date.

If you run into issues while using the documentation, have general feedback, or want to make a suggestion, use the documentation feedback mechanism. From any page in the Cluster Director documentation, click the Send feedback button near the top right of the page. Then, select Documentation feedback.

For documentation feedback, we recommend that you provide the following information (if applicable):

What URL are you reporting?
What was inaccurate?
General comments or suggestions
Your comments will be reviewed by the Cluster Director team.Cluster Director locations
Cluster Director is available in the following regions. However, not all resources that Cluster Director supports are available in each of regions listed in this document.

For more information about regions and zones, see Geography and regions. For more information about the supported regions and zones of the GPU resources that Cluster Director supports, GPU availability by regions and zones.

Regions
The following table lists the regions in the Americas where Cluster Director is available.

Region description	Region name
Oregon	us-west1
Las Vegas	us-west4
Iowa	us-central1
South Carolina	us-east1
South Carolina	us-east1
North Virginia	us-east4
Columbus	us-east5
Dallas	us-south1
The following table lists the regions in Europe where Cluster Director is available.

Region description	Region name
Belgium	europe-west1
Netherlands	europe-west4
Finland	europe-north1
The following table lists the regions in Asia Pacific where Cluster Director is available.

Region description	Region name
Singapore	asia-southeast1
Cluster Director documentation
Read product documentation
You can deploy, manage, and monitor clusters on which you want to run artificial intelligence (AI), machine learning (ML), or high performance computing (HPC) workloads by using Cluster Director. Cluster Director is a Google Cloud product that automates the complex setup and configuration of clusters, helping you configure compute, networking, and storage resources for your clusters to maximize performance and minimize downtimes.

Cluster Director is available by invitation only. If you'd like to request access to Cluster Director in your Google Cloud project, contact your sales representative.
View free product offers
Keep exploring with 20+ always-free products
Access 20+ free products for common use cases, including AI APIs, VMs, data warehouses, and more.

Documentation resources 
Find quickstarts and guides, review key references, and get help with common issues.
format_list_numbered
Guides
Quickstart

Overview

Create a cluster

find_in_page
Reference
gcloud commands

REST API

info
Resources
Support

Locations

Supported networking services in Cluster Director
This document provides a conceptual overview of the high-performance network architecture and mandatory Virtual Private Cloud (VPC) network requirements for the clusters that you deploy in Cluster Director. This information helps you understand how Cluster Director minimizes potential downtimes.

For the tightly coupled, distributed workloads that run on Cluster Director clusters, even minor increases in network latency can lead to significant downtimes. The networks services that Cluster Director uses are designed to minimize any potential downtimes.

Cluster Director network architecture
Cluster Director uses hierarchical, rail-aligned network architecture to provide predictable, high-performance connectivity that minimizes communication overhead. This design helps allow GPUs to spend more time on computation by decreasing the time spent waiting for data.

Cluster Director network architecture is organized as follows to help ensure low-latency communication:

Node or host: a single physical server machine in the data center. Each host has its associated compute resources such as accelerators. The number and configuration of these compute resources depend on the machine family. Compute Engine provisions virtual machine (VM) instances on top of a physical host.

Sub-blocks: a sub-block consists of hosts physically located on a single rack and connected by a top-of-rack (ToR) switch. This setup enables efficient, single-hop communication between any two GPUs in the sub-block.

Blocks: a block consists of multiple sub-blocks interconnected with a non-blocking fabric, providing high-bandwidth connectivity. Any GPU in a block can be reached in a maximum of two network hops.

Clusters: clusters are formed by multiple interconnected blocks. Clusters can scale to thousands of GPUs for large-scale training workloads.

Network traffic separation
To help ensure that different types of traffic don't compete for the same system resources, the network architecture separates high-performance GPU communication from general-purpose host traffic.

GPU-to-GPU communication: this traffic uses dedicated high-speed network interfaces. For accelerator-optimized machine series, this traffic is handled by NVIDIA NICs using technologies like RDMA over Converged Ethernet (RoCE) for efficient, low-latency data exchange directly between GPUs.

Host and storage data plane: all other traffic, including access to Google Cloud services like Cloud Storage, host-level management, and storage access, flows through Titanium NICs on a separate network path.

VPC network and firewall requirements
When you create a cluster, you can either have Cluster Director create a new VPC network for you or use an existing one.

New network: if you let Cluster Director create a network, then it automatically configures the necessary firewall rules for you.

Existing network: if you use an existing network, then you must manually configure the network to meet the mandatory requirements that are described in the following section.

Mandatory configuration for existing networks
If you use an existing network for your cluster, then the network must meet all of the following mandatory requirements:

Private Google Access: the subnetwork must have Private Google Access enabled for the compute nodes to function correctly. For instructions, see Configure Private Google Access.

Firewall rules: you must manually configure the network's firewall rules to allow two types of traffic:

SSH access from IAP: this configuration allows users to connect to login nodes by using SSH. Configure a firewall rule to allow ingress traffic on TCP port 22 from the specific IP address range used by Identity-Aware Proxy (IAP).

Internal communication: this configuration allows nodes within your cluster to communicate with each other. A firewall rule must be in place to permit all ingress traffic (TCP, UDP, and ICMP) from the cluster's own subnetwork IP address range.

For more information, see Use VPC firewall rules.

Multi-VPC configuration
Some high-performance machine types, such as A4 and A3 Ultra machine types, must separate traffic across a multi-VPC environment. This separation is due to a specialized hardware design that physically separates high-speed GPU traffic from general-purpose traffic. Cluster Director handles the complexity of managing the default VPC for general traffic and additional VPCs for GPU-to-GPU traffic.

What's next?
Supported storage services in Cluster Director

Try out Cluster Director
Slurm orchestration in Cluster Director
This document explains the architecture and core capabilities of the managed Slurm environment that Cluster Director uses to handle cluster management, job scheduling, and workload orchestration.

Slurm is a highly scalable and fault-tolerant open-source cluster manager and job scheduler. It's an open-source cluster management and job scheduling system with built-in integration and management within the Cluster Director platform. Slurm is a standard orchestrator for artificial intelligence (AI), machine learning (ML), and high performance computing (HPC) workloads.

Slurm architecture in Cluster Director
When you create and deploy a cluster, Cluster Director automatically sets up a pre-configured Slurm environment for you. This environment consists of the following nodes:

Controller node: the primary control plane component that monitors resources, manages system state, and processes all submitted jobs. The Slurm controller is deployed in a fault-tolerant configuration to help ensure high availability and reliability.

Login nodes: the primary entry points for users to interact with the cluster. You connect to a login node by using SSH to submit jobs, review job statuses, and manage your workflows. The login nodes are pre-configured with the necessary Slurm commands and have access to the shared file systems.

Compute nodes: these nodes are responsible for executing your jobs. Slurm manages these nodes in partitions, which are logical groupings of nodes with specific characteristics (such as machine type or GPU model).

Key Slurm capabilities
Slurm provides several key capabilities that are essential for running large-scale AI, ML, or HPC workloads as follows:

Job scheduling and queuing: Slurm manages a queue of jobs submitted by users. It allocates resources to these jobs based on priority, policies, and resource availability. This approach helps ensure that your cluster efficiently uses resources.

Resource management: Slurm has a detailed awareness of the cluster's resources, including vCPUs, memory, and GPUs. When you submit a job, you can request the specific resources your job needs, and Slurm helps ensure it runs on nodes that meet those requirements.

Topology-aware workload placement: Cluster Director uses Slurm's topology-aware scheduling capabilities to run your workloads. By leveraging information about the physical network layout, Slurm can colocate tasks of a job on VMs that are close to each other in the network. This approach minimizes latency and is critical for the performance of tightly-coupled distributed training workloads.

Fault tolerance: the managed Slurm environment is designed for resilience. Together with autohealing features in Cluster Director, Slurm handles host errors by automatically rescheduling jobs and replacing failed nodes, providing a resilient environment for long-running and critical workloads.

What's next?
Cluster creation process overview

Try out Cluster DirectorCluster Director overview
You can deploy, manage, and monitor clusters that you want to run artificial intelligence (AI), machine learning (ML), or high performance computing (HPC) workloads on by using Cluster Director. Cluster Director is a Google Cloud product that automates the complex setup and configuration of clusters, helping you configure compute, networking, and storage resources for your clusters to maximize performance and minimize downtimes.

Cluster Director is designed for IT administrators and AI researchers who want to avoid the overhead of managing a cluster, while focusing on running their AI, ML, or HPC workloads.

Cluster Director key features
Cluster Director integrates multiple Google Cloud services into a cohesive system that simplifies cluster management and provides the following features:

Automated cluster lifecycle management: the Cluster Director platform is built around a unified control plane with its own API and UI in the Google Cloud console. This feature is a central place to perform all cluster lifecycle operations, letting you programmatically deploy, manage, and scale clusters with all their necessary compute, networking, and storage resources.

Managed cluster orchestration and scaling: Cluster Director uses a managed Slurm environment for fault-tolerant and scalable job scheduling. This managed environment lets you dynamically scale your cluster by adding and editing machine configurations to an existing cluster to adjust resources based on your workload demands.

Resilient, performance-optimized infrastructure: clusters are built on a foundation of performance-optimized hardware, including compute-optimized and accelerator-optimized virtual machine (VM) instances. The system uses topology-aware scheduling to colocate VMs and minimize network latency. It also features built-in resiliency, with autohealing capabilities that automatically detect and replace failed VMs to help minimize downtimes.

Integrated observability: Cluster Director provides built-in tools for monitoring the health, topology, and performance of your clusters through a dashboard in the Google Cloud console. These tools give you an at-a-glance view of your cluster's component health.

Pricing
There is no charge for using Cluster Director itself. Instead, you incur charges for the underlying Google Cloud resources that your clusters use, such as compute, storage, and networking resources. For more information, see the related pricing documentation:

Compute Engine pricing

Cloud Storage pricing

Google Cloud networking services

What's next?
Supported compute resources in Cluster Director

Try out Cluster Director

Cluster creation process overviewReserve capacity through your account team
Preview — Future reservations for blocks of capacity

This feature is subject to the "Pre-GA Offerings Terms" in the General Service Terms section of the Service Specific Terms. Pre-GA features are available "as is" and might have limited support. For more information, see the launch stage descriptions.

This document explains how to reserve capacity for creating virtual machine (VM) instances with GPUs attached in compute nodes. You do so by using future reservations for blocks of capacity. To learn about all the options to obtain capacity in Cluster Director, see Capacity overview.

To help ensure that you have the resources to create GPU VMs in a cluster partition, you must do the following:

Request a future reservation from Google. This action lets you reserve blocks of capacity for a defined duration, starting on a specific date and time that you choose.

Review the draft request created by Google. Based on your request, Google creates a future reservation request. You can then review the request and, if needed, contact your account team to make changes.

You submit the request. After you submit the request, Google Cloud approves it within a few minutes. Then, Compute Engine automatically creates (auto-creates) an empty reservation.

At your request start time, Compute Engine delivers the reserved resources. Compute Engine provisions your requested capacity into the auto-created reservation. You can then use the reservation to create GPU VMs in your cluster until the reservation period ends.

Limitations
This section describes the limitations for future reservation requests, and for the auto-created reservations for a request.

Limitations for future reservation requests
After Google creates a draft future reservation request for you, the following limitations apply:

You can't modify the request details, including the share type.

After the request is submitted, approved, and its state changes to PROVISIONING, you can no longer cancel or delete the request. You commit to pay for the requested capacity from the request's start time, regardless of usage.

Limitations for auto-created reservations
After Compute Engine creates an on-demand reservation to fulfill your requested capacity, the following limitations apply:

You can only use the reservation after the request start time.

You can't manually modify the reservation. For your available options, contact your account team.

You can't manually delete the reservation. When you reserve capacity, if you specify that you don't want to automatically delete the reservation at the end of its reservation period, you must contact your account team to delete the reservation.

Before you begin
To use the samples on this page, you might need to authenticate to Google. Select one of the following options to check what you need to do:

Console
gcloud
REST
To run the commands on this page, set up the Google Cloud CLI in one of the following development environments:

To use an online terminal with the gcloud CLI already set up, activate Cloud Shell:

Activate Cloud Shell on this page

A Cloud Shell session starts and displays a command-line prompt. Initializing the session can take a few seconds to complete.

To use a local development environment, complete the following these steps:

If you haven't already, then install the gcloud CLI.

If you're using an external identity provider (IdP), then sign in to the gcloud CLI with your federated identity.

Initialize the gcloud CLI:



gcloud init
If you have previously installed the gcloud CLI, then verify that you have the latest version:



gcloud components update
Required roles
To get the permissions that you need to create a future reservation request, ask your administrator to grant you the Compute Future Reservation User (roles/compute.futureReservationUser) IAM role on the project. For more information about granting roles, see Manage access to projects, folders, and organizations.

This predefined role contains the permissions required to create a future reservation request. To see the exact permissions that are required, expand the Required permissions section:

Required permissions
You might also be able to get these permissions with custom roles or other predefined roles.

Quota
As part of the future reservation request process, Google manages quota for your reserved resources. You don't need to request quota. At the start time of your approved future reservation, Google automatically increases your quota if you lack it for the reserved resources.

Request capacity through your account team
Contact your account team and provide the following information for Google to create a draft future reservation request:

Project number: the number of the project where your account team creates the request and Compute Engine provisions the capacity.

Machine type: the machine type to reserve. You can specify one of the following:

A4X (a4x-highgpu-4g)

A4 (a4-highgpu-8g)

A3 Ultra (a3-ultragpu-8g)

A3 Mega (a3-megagpu-8g)

Zone: the zone where you want to reserve capacity. To review the available regions and zones for a GPU machine type, see GPU regions and zones.

Total count: the total number of VMs to reserve. You can only reserve multiples of two VMs. Block sizes and VM count per block vary based on machine type and availability. Your account team can provide more details for your request.

Start time: the start time of the reservation period. You can start using the reserved capacity at that time. Format the start time as a RFC 3339 timestamp as follows:



YYYY-MM-DDTHH:MM:SSOFFSET
Replace the following:

YYYY-MM-DD: a date formatted as a four-digit year, two-digit month, and a two-digit day of the month, separated by hyphens (-). For example, to specify December 31, 2025, use 2025-12-31.

HH:MM:SS: a time formatted as two-digit hours (24-hour time), two-digit minutes, and two-digit seconds, separated by colons (:).

OFFSET: the time zone formatted as an offset of Coordinated Universal Time (UTC). For example, to use the Pacific Standard Time (PST), specify -08:00. To use no offset, specify Z.

End time: the end time of the reservation period. Format it as an RFC 3339 timestamp. At that time, Compute Engine does the following:

Compute Engine deletes the auto-created reservation.

Based on the termination action that you specify for your VMs, Compute Engine stops or deletes any VMs that you created by using the auto-created reservation.

Reservation name: the name of the reservation that Compute Engine creates to deliver your reserved capacity. Compute Engine can only create specifically targeted reservations.

Reservation automatic deletion: whether you want Compute Engine to automatically delete the auto-created reservation at the end of the reservation period, or at a later time. If you want to manually delete the reservation, then you must contact your account team to delete the reservation.

Share type: whether only your project can use the auto-created reservation (LOCAL), or other projects can use the reservation (SPECIFIC_PROJECTS). This property can't change after you submit the request. To share reserved capacity with other projects in your organization, do the following:

If you haven't already, then verify that the project where Google creates the request is allowed to create shared reservations.

Provide the numbers of the projects to share the reserved capacity with. You can specify up to 100 projects in your organization.

Commitment name: if your reservation period is one year or longer, then you must purchase and attach a resource-based commitment to your reserved resources. You can purchase a commitment with a 1-year or 3-year plan. If you share the reserved capacity with other projects, then those projects get discounts only if they use the same Cloud Billing account as the project where you reserve capacity. For details, see Enable CUD sharing for resource-based commitments.

Important: A commitment becomes active at 12:00 AM US and Canadian Pacific Time (UTC-8, or UTC-7 during daylight saving time) on your chosen start date. If you specify a different start time, then the commitment becomes active at 12:00 AM on the following day. For example, if you specify a start time at 3:00 PM on December 1, 2025, the commitment becomes active at 12:00 AM on December 2, 2025.
When Google creates the draft future reservation request, your account team contacts you.

Review and submit a draft reservation request
After you provide the type and amount of resources to reserve to your account team, Google creates a draft future reservation request. You can review the draft request and, if correct, submit it for review. You must submit the request before the request start time.

Caution: When you submit a request, you confirm your commitment to reserve your requested capacity. After Google Cloud approves the request, and the request state changes to PROVISIONING, you can't cancel, modify, or delete the request. You commit to pay for the requested capacity from the request's start time, regardless of usage.
To review and submit a draft future reservation request, select one of the following options:

Console
gcloud
REST
To view a list of future reservation requests in your project, use the gcloud beta compute future-reservations list command with the --filter flag set to PROCUREMENT_STATUS=DRAFTING:



gcloud beta compute future-reservations list --filter=PROCUREMENT_STATUS=DRAFTING
In the command output, look for the reservation request that has the name that you provided to your account team.

To view the details of the draft request, use the gcloud beta compute future-reservations describe command.

Select and run one of the following commands:

curl
Powershell
cmd.exe


gcloud beta compute future-reservations describe FUTURE_RESERVATION_NAME \
    --zone=ZONE
Replace the following:

FUTURE_RESERVATION_NAME: the name of the draft future reservation request.

ZONE: the zone where Google created the request.

The output is similar to the following:


autoCreatedReservationsDeleteTime: '2026-02-10T19:20:00Z'
creationTimestamp: '2025-11-27T11:14:58.305-08:00'
deploymentType: DENSE
id: '7979651787097007552'
kind: compute#futureReservation
name: example-draft-request
planningStatus: DRAFT
reservationName: example-reservation
schedulingType: INDEPENDENT
selfLink: https://www.googleapis.com/compute/beta/projects/example-project/zones/europe-west1-b/futureReservations/example-draft-request
selfLinkWithId: https://www.googleapis.com/compute/beta/projects/example-project/zones/europe-west1-b/futureReservations/7979651787097007552
specificReservationRequired: true
specificSkuProperties:
  instanceProperties:
    guestAccelerators:
    - acceleratorCount: 8
      acceleratorType: nvidia-h200-141gb
    localSsds:
    - diskSizeGb: '375'
      interface: NVME
    ...
  machineType: a3-ultragpu-8g
totalCount: '2'
status:
  autoCreatedReservations:
  - https://www.googleapis.com/compute/beta/projects/example-project/zones/europe-west1-b/reservations/example-reservation
  fulfilledCount: '2'
  lockTime: '2026-01-27T19:15:00Z'
  procurementStatus: DRAFTING
timeWindow:
  endTime: '2026-02-10T19:20:00Z'
  startTime: '2026-01-27T19:20:00Z'
zone: https://www.googleapis.com/compute/beta/projects/example-project/zones/europe-west1-b
In the command output, verify that the request details, such as the reservation period and share type, are correct. Additionally, if you purchased a commitment, verify that it's specified. If the details are incorrect, then contact your account team.

To submit the draft request for review, use the gcloud beta compute future-reservations update command with the --planning-status flag set to SUBMITTED.

Select and run one of the following commands:

curl
Powershell
cmd.exe


gcloud beta compute future-reservations update FUTURE_RESERVATION_NAME \
    --planning-status=SUBMITTED \
    --zone=ZONE
Within a few minutes, Google Cloud approves your request, and then Compute Engine creates an empty reservation with your requested resources.

What's next
View reserved capacitygcloud alpha cluster-director
NAME
gcloud alpha cluster-director - manage Cluster Director resources
SYNOPSIS
gcloud alpha cluster-director GROUP [GCLOUD_WIDE_FLAG …]
DESCRIPTION
(ALPHA) Manage Cluster Director resources.
GCLOUD WIDE FLAGS
These flags are available to all commands: --help.
Run $ gcloud help for details.

GROUPS
GROUP is one of the following:
clusters
(ALPHA) Manage Cluster Director cluster resources.
locations
(ALPHA) View Locations resources.
operations
(ALPHA) Manage Operation resources.
NOTES
This command is currently in alpha and might change without notice. If this command fails with API permission errors despite specifying the correct project, you might be trying to access an API with an invitation-only early access allowlist.

Context
The purpose of this document is to provide an overview of the GCE maintenance product for the accelerator family of VMs (A*), covering details about software maintenance and hardware repairs/maintenance. It discusses the types of maintenance events customers can expect on their instances, the tools GCE provides to manage them, and recommendations on how to manage maintenance using these tools while minimizing impact to workloads.The intended audience for this document is account teams, field teams and eventually customers when summarized in a different format. 

GCE GPU VMs are offered in two consumption modes, High Availability mode (aka managed mode) and All Capacity mode (aka raw mode). High availability mode is offered for A3 Ultra (H200 GPUs) and A4 (B200 GPUs) VM types, whereas full capacity mode is offered for A4x (GB200 GPUs) VM types (and future GB300 GPU based instance families). Maintenance types and best practices recommended below are applicable to GPU VM families (like A3 Ultra /A4) with High availability consumption mode. 
In Highly Availability Capacity mode, GCE will automatically move instance(s) to a different host machine in case of a failure that needs a repair. 
In All Capacity mode, GCE will keep the instance(s) pinned to the same machine till the completion of the repair. 
Customers can override the latter behavior by stopping their instance; however, the instance(s) can only start on the capacity available to the customer. 
Current state of GCE GPU maintenance types
To help explain the GCE GPU maintenance and reliability landscape, we want to break down things in two broad categories: 
Software maintenance - Updates and bug fixes to the software/firmware that manages the GCE GPU infrastructure
Hardware maintenance/repairs - Hardware repairs to fix or replace any broken hardware components

For each of these categories, there are sub categories based on the content, severity and ability to detect the issues: 
Planned maintenance - A routine and periodic maintenance to improve the reliability and performance of the infrastructure. GCE knows well in advance about these planned rollouts, and hence can notify customers with a full notification period that is in accordance with our uptime guarantees and frequencies (example). Depending on the instance family, customers may get access to customer-triggered maintenance (perform-maintenance API) to initiate maintenance on their own schedule.
Emergent maintenance - Medium severity issues (e.g. hardware degradation, critical software bug fixes) and vulnerabilities that don't need immediate resolutions but cannot wait till next planned maintenance to be resolved. Customers will typically get a shorter notification period with expected maintenance start and end date. Similar to planned maintenance, depending on the instance family, customers may be provided control to initiate maintenance on demand. 
Critical failures - Unexpected software or hardware failures that cannot be detected in advance that need to be fixed immediately. Customers will get very little or no advanced notice and will not have any control on the initiation of the repairs/maintenance. 

A simple visual representation of different maintenance types is shown below: 


GCE GPU Software Maintenance Management
In this section we give an overview of the current software maintenance feature set and followed by recommendations for different workload types. 

For Cluster Director managed GPU VM families (A3 Ultra, A4 and beyond), we provide customers with special controls that allow them to schedule maintenance for software updates across their entire reservations. For guidance on the controls and notifications, see following documentation:
https://cloud.google.com/ai-hypercomputer/docs/manage/host-events (per instance APIs)
https://cloud.google.com/ai-hypercomputer/docs/manage/host-events-reservations (reservation APIs) 

Below table gives an overview software maintenance for A3 Ultra/A4 and A4x GPU VMs. 



Planned Software Maintenance
Emergent Software Maintenance
Critical failures
Example content
Newer version of the Google software, kernel updates
Fixes for security vulnerabilities with a fixed end dates
Bugs that crash the instance
Frequency
Every quarter
No fixed frequency
No fixed frequency
Expected downtime
4 hours
4 hours
Undeterministic (Case specific)
Advanced notice
90 days
7 days
Not Available
Notification mechanism
Cloud logging, gcloud CLI/API, instance’s metadata server
Cloud logging, gcloud CLI/API, instance’s metadata server
Cloud logging, after the event
Maintenance controls
Perform-maintenance API, Group maintenance at entire reservation, block, subblock
Perform-maintenance API, Group maintenance at entire reservation, block, subblock
Not Available
LocalSSD data retention
Data retained (instances will be terminated and restarted in the same host during maintenance) (Default behavior)
Data retained (instances will be terminated and restarted in the same host during maintenance) (Default behavior)
Retention attempted based on the localSSDrecoverytimout configuration, retention not guaranteed if host needs additional repairs. 


Below are our recommendations for managing software planned maintenance based on the workload type based on the workload characteristics and performance expectations. 
Best practice #1: Recommendation for training workloads 
Suitable for: Training workloads that run across a large set of instances where the job can only function with all instances available, and consistent performance across all nodes is critical. Synchronized maintenance provides the most optimal downtime by taking all instances down at the same time.
Maintenance approach: Group maintenance at reservation, block or subblock level 
Maintenance controls: perform maintenance API targeted for reservation, block or subblock, with specific scope like unused machines, running instances, all instances or per instance
Recommended workflow: 
GCE sends pending maintenance notification for the entire reservation
Customers identify the targeted GPU subblock/group that needs to be upgraded first using the grouping mechanism: all, running, unused, block, subblock.
Initiate perform maintenance API on the identified subblock or other cluster topology
The targeted set of machines will be down during the notified maintenance window for  the maintenance operation
Benefits: 
Synchronous updates across all the participating nodes
Consistent, uniform performance across all the nodes participating in training job
Entire group is taken down together for maintenance thus reducing overall downtime impact for that group
LocalSSD data is retained (by default)
Works for High Availability mode and Full capacity mode VM/instance families
Considerations:
Full cluster impact - entire group is down for ~4 hours (current SLO) impacting availability of GPU resources
CUDs are billed for this duration of planned maintenance downtime
After a node failure, the replacement machine could be running the newer version of software so a mixed software experience is unavoidable. 
Best practice#2: Recommendation for inference or serving workloads
Suitable for: Inference workloads that need most of the instances available all of the time in order for jobs to function. While consistent performance across all nodes is important, ensuring that the number of instances remain available during maintenance conforms to the workload’s error budget is critical to avoid disrupting inference or serving workloads.
Maintenance approach: Staggered maintenance at per instance or subset of instances within the reservation
Maintenance controls: perform maintenance API at per instance level or subset of instances - perform-maintenance API support multiple instances being included in the operation
Recommended workflow: 
GCE sends pending maintenance notification for entire reservation
Customers identify/choose a sequence and a rate of per instance maintenance operation that their serving or inference workload can support
Initiate perform maintenance API on the identified set of instances 
Continue this operation till maintenance is completed across all the nodes within the cluster
Benefits: 
Customer can choose which machines to rotate through maintenance avoiding instance downtime impact the throughput
LocalSSD data is retained (by default)
Customers can control the rate and sequence of the instance maintenance
Works for managed mode (high availability) and raw (full capacity) mode instance families
Considerations: 
Per instance downtime of 4 hours could still be an unavailability concern
Rolling updates means that different nodes will be on different software versions
This option is not appropriate for instance families that require Subblock-level (NVSwitch Domain) updates where NVIDIA requires the entire domain/sublock run the same versions.
GCE GPU Hardware Repairs/Maintenance Management
Similar to software maintenance, GCE has to perform maintenance and repair operations on hardware infrastructure to maintain optimal performance and reliability. In most cases, GCE performs infrastructure maintenance non-disruptively, and customers are not aware of the updates for upstream systems from their instances; however, in some cases, disruption may be required. Disruptive hardware maintenance can be categorized into below subcategories: 

Planned hardware maintenance - These are types of infrastructure maintenance or repairs that GCE is aware of in advance and therefore can be planned. Within this, larger scale datacenter level infrastructure maintenance operations that may be disruptive will be communicated with another incident management channel like Personalized Service Health (Cloud Hub), which is outside the scope of this document. GPU infrastructure specific (e.g. new firmware, drivers) maintenance that requires disruption is combined with a planned software maintenance. Fleetwide GPU scans and repairs are a special category of planned hardware maintenance covered in detail here. 
Fleet wide GPU Scans/Repairs - To address unavoidable reliability issues that impact a large portion of the fleet like GPU thermal problems, GPU firmware issues or a MCE/hardware security vulnerability. This also covers GPU periodic scans that are recommended by Nvidia for improved reliability. Refer to the recommendation section below to learn more about this type of hardware repair flow. 
Emergent hardware maintenance - Hardware emergent maintenance experience shares similarities to planned software and emergent software maintenance where customers will be provided with advanced maintenance notification - typically 7 days - and provided with control for customers to initiate the repair on-demand. Hardware emergent maintenance supports unexpected hardware failures, typically component level failures (e.g. GPU failure/XID error), or scenarios that require near-term attention, that will not immediately impact the availability of the instance, but are unable to wait until the next planned maintenance window. Compared to software emergent maintenance events, a key characteristic of a hardware emergent maintenance event is that the instance has to be restarted on another host machine as the underlying faulty host needs to be repaired. 
Critical failures/outages - These are the critical failures like host hardware crashes, datacenter outages etc., Based on GCE’s ability to detect these, there are two categories of hardware failures: 
Host errors or outages: These are the failures where GCE cannot detect early symptoms and hence systems will just crash causing an immediate instance disruption. A post failure notification is communicated in the log file as a ‘hosterror’. Refer to public docs for more details - https://cloud.google.com/compute/docs/instances/host-maintenance-overview#hosterror.  
High severity failures: These are failures where GCE systems can detect some symptoms, and our categorization maps these to high severity failure symptoms, we terminate the running instance with 10 minute notice. There will be a terminateOnHostMaintenance event reported in the log file after the event has occurred. 

Below table summarizes the experience across these hardware maintenance/failure scenarios. 




Planned/Routine Hardware maintenance
Fleet wide GPU scans/repairs
Emergent Hardware maintenance
Critical failure
Example scenarios
Datacenter maintenance, service maintenance, Power supply or rack maintenance
GPU thermal screening, Fleet wide GPU hardware replacement
XID errors, Specific GPU failure scenarios like HotGPU, NVlink domain failure
Crashes due to uncorrectable CPU or memory errors (e.g. unhandled machine checks or PCIe errors), TOR failure, uplink failure, localSSD failure, 

large scale data center outage (e.g. power loss to datacenter) 
Notification mechanism
For Disruptive, typically included as part of Planned Maintenance: Cloud logging, gcloud CLI/API, instance’s metadata server

For larger scale, typically delivered via Personalized Service Health (Cloud Hub)
Manual communication via account teams/emails
Cloud logging, gcloud CLI/API, instance’s metadata server
Cloud logs (hosterror or terminateOnHostMaintenance)

For larger scale, typically delivered via Personalized Service Health (Cloud Hub)
Advanced communication
Yes, no specific details on # of days in advance
No fixed cadence, manual coordination
7 days
NA
Customer control
For per-instance, Perform maintenance API and ReportFault API.

For large scale, no.
Instance termination, ReportFaulty API
Perform maintenance API
NA
LocalSSD data retention
Yes
No, customers are expected to retain LSSD data
No, customers are expected to retain LSSD data
Retention attempted based on the localSSDrecoverytimout configuration, retention not guaranteed if host needs additional repairs. 


Best practice#3: Recommendation for fleetwide GPU hardware reliability issues/scans
Suitable for: Fleet wide hardware scans that take in the O(hours) and include repairs or hardware swaps needed to improve reliability of the infrastructure. For example, running GPU scans for SDCs orGPU thermal issues, or a specific hardware failure symptom that affects all the GPUs across the fleet. 
Maintenance approach: Manual coordinated and staggered maintenance utilizing spare machines in the subblock
Maintenance controls: Report Faulty host API, manual localSSD data copy (optional), Pending scans API
Recommended workflow:
GCE shares pending repairs and/or scan information with the customers (number of machines, repair timelines etc.,)
For scans, GCE also provides an API to check pending scans per instance across the cluster. 
Customers work with the GCE team to orchestrate and sequence the order to send the machines to repair, and align on a disruptive budget in terms of number of instances per day/per week. 
Once aligned, customers call Report faulty host API on the instances in that sequence at the rate communicated by the GCE team. 
Benefits: 
Only feasible option to churn through the entire fleet for higher blast radius hardware failures
Opportunistic upgrade allows GCE to manage a healthy pool of spare machine with completed screening and repairs
Considerations: 
For Raw/full capacity mode, there are no spare machines to swap, so instances will stay in REPAIRING state till scans/repairs are completed. 
Rate of machines to be swapped with spare depends on many factors, thus needing additional manual coordination to scan/repair through all the nodes in the cluster. 
