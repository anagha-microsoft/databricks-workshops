This section covers provsioning and all necessary configuration required for each service.<BR><BR>

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

### 3.0.2. Create consumer groups
1.  From the Azure portal, navigate to the IoT hub instance provisioned in 3.0.1.<BR>
2.  Select Endpoints<BR>
3.  From "Built-in endpoints", select "Events"<BR>
4.  Create one consumer group called "kafkaConnect-cg"<BR>
5.  Create one consumer group called "spark-cg"<BR>

### 3.0.3. Capture key information needed for KafkaConnect and Spark integration
Capture the following:<br>
1. Event Hub-compatible name; e.g. ```bhoomi-telemetry-simulator```
2. Event Hub-compatible endpoint (starts with ```sb://```, ends with ```.servicebus.windows.net/```; e.g. ```sb://iothub-ns-bhoomi-tel-855096-4db209c4e3.servicebus.windows.net/```
3. Partitions; E.g. ```4```
4. Shared access key<br>
On the portal, inside your IoT hub service, go to the left navigation panel and select "Shared Access Policies".  Then click on the policy "service", and capture the primary key

# 4. HDInsight Kafka
### 4.0.1. Provision a HDInsight Kafka cluster
Follow the steps in the [documentation](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-get-started#create-a-kafka-cluster) to create a cluster from the Azure portal.

### 4.0.2. Configure Kafka 
##### 1.  Configure Kafka for IP advertising<br>
By default, Zookeeper returns the domain name of the Kafka brokers to clients. This configuration does not work for DNS name resolution with Vnet peering. The [documentation](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-connect-vpn-gateway#configure-kafka-for-ip-advertising) steps 1-5, at the link, will detail configuring Kafka to advertise IP addresses instead of domain names.  Make the change but do not restart Kafka yet, till you complete the next step.

##### 2. Configure Kafka listener to listen on all network interfaces
Refer step 7, in this [documentation](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-connect-vpn-gateway#configure-kafka-for-ip-advertising) configure Kafka listener to listen on all network interfaces.  You can now restart Kafka service.

##### 3. Get the private IPs of the Kafka brokers
This will be useful for direct integration from Spark.<br>
Go to Ambari->Hosts, and look for the listing of brokers.  The names will start with ```wn```.  Get the private IPs.
The following is the author's broker list.<br>
```10.1.0.5:9092,10.1.0.7:9092,10.1.0.10:9092,10.1.0.14:9092```

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
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 8 --topic iot_telemetry --zookeeper $ZOOKEEPER_HOSTS
```
### 4.0.4. Deploy an edge node to your existing Kafka cluster
Follow the instructions at [this](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-apps-use-edge-node#add-an-edge-node-to-an-existing-cluster) link to deploy an edge node that is automatically joined to the cluster.<BR>
  
Once the deployment completes, in the Ambari, hosts page, you should see the edge node listed.

### 4.0.5. Install/configure **standalone** KafkaConnect for Azure IoT Hub
In the same terminal, where you have SSH'd into the head node of the Kafka cluster, while SSH'd into head node, SSH to the private IP of the edge node.  You can locate the private IP of the edge node from the Ambari - hosts page.<br>

e.g. 
```
ssh akhanolk@10.1.0.8
```
<br>

4.5.0.1.  Create a directory on the edge node to download the connector to:
```
mkdir -p opt/kafkaConnect
cd opt/kafkaConnect
```
4.5.0.2.  Download the latest connector from here-<br>
https://github.com/Azure/toketi-kafka-connect-iothub/releases/.

At the time of authoring this lab..<br>
```
cd ~/opt/kafkaConnect
wget "https://github.com/Azure/toketi-kafka-connect-iothub/releases/download/v0.6/kafka-connect-iothub-assembly_2.11-0.6.jar"
sudo cp kafka-connect-iothub-assembly_2.11-0.6.jar /usr/hdp/current/kafka-broker/libs/
```
4.5.0.3.  Create and populate cluster name into a variable:<br>
```
read -p "Enter the Kafka on HDInsight cluster name: " CLUSTERNAME
```

4.5.0.4.  Install jq to process json easily<br>
```
sudo apt -y install jq
```
4.5.0.5.  Get broker list into a variable<br>
```
export KAFKABROKERS=`curl -sS -u admin -G https://$CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$CLUSTERNAME/services/KAFKA/components/KAFKA_BROKER | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2`
```
Validate:
```
echo $KAFKABROKERS
```
4.5.0.6.  Get zookeeper list into a variable<br>
```
export KAFKAZKHOSTS=`curl -sS -u admin -G https://$CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$CLUSTERNAME/services/ZOOKEEPER/components/ZOOKEEPER_SERVER | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2`
```
Validate:
```
echo $KAFKAZKHOSTS
```

4.5.0.7. Edit/add 4 pieces of configuration in the connect-standalone.properties file on the edge node<br>
```
sudo vi /usr/hdp/current/kafka-broker/config/connect-standalone.properties
```
1.  Replace ```localhost:9092``` in ```bootstrap.servers=``` conf to reflect broker-port list from step 4.5.0.5<br>
2.  Replace the ```key.converter=``` to read ``` key.converter=org.apache.kafka.connect.storage.StringConverter```
3.  Replace the ```value.converter=``` to read ```value.converter=org.apache.kafka.connect.storage.StringConverter```
4.  Add a line at the end of the file ```consumer.max.poll.records=100``` to prevent timeouts
<br>

4.0.4.8. List the topics created<br>
```
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper $KAFKAZKHOSTS
```
You should see this..
```
iot_telemetry
```

4.0.4.9. Download the connect-iot-source.properties<br>

```sudo wget -P /usr/hdp/current/kafka-broker/config/ https://raw.githubusercontent.com/Azure/toketi-kafka-connect-iothub/master/connect-iothub-source.properties```


4.0.4.10. Edit the connect-iot-source.properties to reflect the IoT hub source<br>
```
sudo vi /usr/hdp/current/kafka-broker/config/connect-iothub-source.properties
```
Modify the properties as follows:<br>
1.  ```Kafka.Topic=PLACEHOLDER```: Replace ```PLACEHOLDER``` with ```iot_telemetry```. Messages received from IoT hub are placed in the iot_telemetry-in topic.<br>
2.  ```IotHub.EventHubCompatibleName=PLACEHOLDER```: Replace ```PLACEHOLDER``` with the Event Hub-compatible name.<br>
3.  ```IotHub.EventHubCompatibleEndpoint=PLACEHOLDER```: Replace ```PLACEHOLDER``` with the Event Hub-compatible endpoint.<br>
4.  ```IotHub.Partitions=PLACEHOLDER```: Replace ```PLACEHOLDER``` with the number of partitions from the previous steps.<br>
5.  ```IotHub.AccessKeyName=PLACEHOLDER```: Replace ```PLACEHOLDER``` with service.<br>
6.  ```IotHub.AccessKeyValue=PLACEHOLDER```: Replace ```PLACEHOLDER``` with the primary key of the service policy.<br>
7.  ```IotHub.StartType=PLACEHOLDER```: Replace ```PLACEHOLDER``` with a UTC date. This date is when the connector starts checking for messages. The date format is yyyy-mm-ddThh:mm:ssZ.<br>
8.  ```BatchSize=100```: Leave as is. This change causes the connector to read messages into Kafka once there are five new messages in IoT hub.<br>
9. ```IotHub.ConsumerGroup=PLACEHOLDER```: Replace ```PLACEHOLDER``` with ```kafkaConnect-cg```. <br>
10.  Modify the ```connector.class=...``` entry to be ```connector.class=com.microsoft.azure.iot.kafka.connect.IotHubSourceConnector```
<br>
Save and close file.<br>

[Example of connect-iot-source.properties](https://github.com/Azure/toketi-kafka-connect-iothub/blob/master/README_Source.md)
[Full documenation](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-connector-iot-hub)

# 5.  Connect the dots - IoT Hub and Kafka, with KafkaConnect

### 5.0.1. Start the source connector
On the same, HDInsight kafka edge node, on the Linux CLI, run the below.<br>
```/usr/hdp/current/kafka-broker/bin/connect-standalone.sh /usr/hdp/current/kafka-broker/config/connect-standalone.properties /usr/hdp/current/kafka-broker/config/connect-iothub-source.properties```
Do not shut dow this terminal, unless you have launched the process with nohup as a background process.

### 5.0.2. Launch console consumer to quickly test
In a separate terminal window, connect to Kafla cluster headnode and launch the Kafka console consumer to see if the messages are making it.
```
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --bootstrap-server $KAFKABROKERS --topic "iot_telemetry" --from-beginning
```
If you see activity, you are good to proceed with the rest of the lab.

