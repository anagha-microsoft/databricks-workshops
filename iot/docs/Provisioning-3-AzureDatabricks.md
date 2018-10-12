
This section covers provisioning of Azure Databricks.

# 4. Azure Databricks
From the portal navigate to the resource group you created - "Telemetry-Processor".

## 4.0.1.  Provision a storage account for Databricks 
### 4.0.1.1.  Create storage account
Create a general purpose storage account (version 1) in the resource group - Telemetry-Processor.<br>
![SA-1](../../images/CreateStorageAcct-1)

<br>
![SA-2](../images/CreateStorageAcct-2)
<br>
![SA-3](../images/CreateStorageAcct-3)
<br>
![SA-4](../images/CreateStorageAcct-4)
<br><br>

### 4.0.1.2.  Create storage account containers
Within this storage account, provision 3 containers with private (no anonymous access) configuration<br>
![SA-7](../images/CreateStorageAcct-7)
<br>1.  Create a container called raw<br>
![SA-8](../images/CreateStorageAcct-8)
<br>2.  Create a container called curated<br>
![SA-9](../images/CreateStorageAcct-9)
<br>3.  Create a container called consumption<br>
![SA-10](../images/CreateStorageAcct-10)
<br><br>
![SA-11](../images/CreateStorageAcct-11)

### 4.0.1.3. Capture the storage account credentials



### 2.0.2. Provision an Azure Databricks workspace
Provision an Azure Databricks workspace in the resource group - telemetry-processor<br>
[Documentation](https://docs.microsoft.com/en-us/azure/azure-databricks/quickstart-create-databricks-workspace-portal)

### 2.0.3. Provision an Azure Databricks cluster in the workspace
Provision an Azure Databricks cluster with 3 workers with default SKU, with ability to autoscale to 5 workers.<br>
[Documentation](https://docs.microsoft.com/en-us/azure/azure-databricks/quickstart-create-databricks-workspace-portal#create-a-spark-cluster-in-databricks)

### 2.0.4. Set up Vnet peering between Databricks and #1.0.2., for Kafka
Set up peering from the Databricks vnet to the Kafka vnet and vice-versa.<br>
[Documentation on Vnet peering](https://docs.azuredatabricks.net/administration-guide/cloud-configurations/azure/vnet-peering.html#vnet-peering)
Enable forwarding...

#### Attach the dbc

### 2.0.5. Add the Spark - Kafka dependencies to the cluster
Add the Spark Kafka library to the cluster<br>
Find the compatible version on Maven central.  For HDInsight 3.6, with Kafka 1.1/1.0/0.10.1, and Databricks Runtime 4.3, Spark 2.3.1, Scala 2.11, the author used-<br>
org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1

LINK TO DOCS
