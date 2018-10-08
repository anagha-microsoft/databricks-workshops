# 1.  General Azure
### 1.0.1. Provision a resource group
### 1.0.2. Provision a virtual network

# 2. Azure Databricks
### 2.0.1. Provision a storage account
### 2.0.2. Provision an Azure Databricks workspace
### 2.0.3. Provision an Azure Databricks cluster in the workspace
### 2.0.4. Set up Vnet peering between Databricks and #1.0.2., for Kafka

# 3. Azure IoT Hub
### 3.0.1. Provision the device telemetry generator 
This will create an Azure IoT hub with Azure Cosmos DB as device registry.

# 4. HDInsight Kafka
### 4.0.1. Provision a HDInsight Kafka cluster
Follow the steps in the [documentation](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-get-started#create-a-kafka-cluster) to create a cluster from the Azure portal.

### 4.0.2. Configure Kafka 
##### 1.  Configure Kafka for IP advertising<br>
By default, Zookeeper returns the domain name of the Kafka brokers to clients. This configuration does not work for DNS name resolution with Vnet peering. The [documentation](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-connect-vpn-gateway#configure-kafka-for-ip-advertising) steps 1-5, at the link, will detail configuring Kafka to advertise IP addresses instead of domain names.  Make the change but do not restart Kafka yet, till you complete the next step.

##### 2. Configure Kafka listener to listen on all network interfaces
Refer step 7, in this [documentation](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-connect-vpn-gateway#configure-kafka-for-ip-advertising) configure Kafka listener to listen on all network interfaces.  You can now restart Kafka service.

### 4.0.3. Create a Kafka topic
On the Azure portal, go to your resource group and click on the resource for Kafka.  In the left hand navigation panel, you should see "SSH + cluster login".  Click on it, then select the default entry (headnode) in the hostname drop down and you should see the textbox directly below populated with SSH command.  Copy it, launch a bash terminal and connect to HDInsight Kafka. 
E.g.
ssh <yourClusterUser>@<yourClusterName>-ssh.azurehdinsight.net

Once connected, run the below to create a topic.
##### 1.  Get the Zookeeper server list
Run this on the terminal of the headnode of your Kafka cluster to get the zookeeper server list with port number.
This is required for creating a Kafka topic in your Kafka cluster from the CLI.
```
CLUSTERNAME="WHATEVER_YOUR_CLUSTERNAME_IS"
ZOOKEEPER_HOSTS=`curl -u admin -G "https://$CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$CLUSTERNAME/services/ZOOKEEPER/components/ZOOKEEPER_SERVER" | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2`
```

##### 2.  Create a topic
```
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 10 --topic iot_telemetry --zookeeper $ZOOKEEPER_HOSTS
```
### 4.0.4. Deploy an edge node to your existing Kafka cluster
Follow the instructions at [this](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-apps-use-edge-node#add-an-edge-node-to-an-existing-cluster) link to deploy an edge node that is automatically joined to the cluster.<BR>
  
Once the deployment completes, in the Ambari, hosts page, you should see the edge node listed.

### 4.0.5. Install/configure KafkaConnect for Azure IoT Hub
In the same terminal, where you have SSH'd into the head node of the Kafka cluster, SSH to the private IP of the edge node.  You can locate the private IP of the edge node from the Ambari - hosts page.<br>

e.g. ssh akhanolk@10.1.0.8<br>
<br>

1.  Create a directory on the edge node to download the connector to:
```
mkdir -p opt/kafkaConnect
cd opt/kafkaConnect
```
2.  Download the latest connector from here-<br>
https://github.com/Azure/toketi-kafka-connect-iothub/releases/.

At the time of authoring this lab..<br>
```
 wget "https://github.com/Azure/toketi-kafka-connect-iothub/releases/download/v0.6/kafka-connect-iothub-assembly_2.11-0.6.jar"
```
3.  Configure by running below<br>

3.1. Create and populate cluster name variable:<br>
```
read -p "Enter the Kafka on HDInsight cluster name: " CLUSTERNAME
```

3.2.  Install jq to process json easily<br>
```
sudo apt -y install jq
```


# 5.  Connect the dots - IoT Hub and Kafka, with KafkaConnect



