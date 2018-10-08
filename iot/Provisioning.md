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
### 4.0.2. Configure Kafka 
##### 1.  Configure Kafka for IP advertising<br>
By default, Zookeeper returns the domain name of the Kafka brokers to clients. This configuration does not work for DNS name resolution with Vnet peering. The documentation at the link wil detail configuring Kafka to advertise IP addresses instead of domain names.  Make the change but do not restart Kafka yet, till you complete the next step.
[Documentation](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-connect-vpn-gateway#configure-kafka-for-ip-advertising)
##### 2. Configure Kafka listener to listen on all network interfaces
Refer step 7, to [documentation](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-connect-vpn-gateway#configure-kafka-for-ip-advertising) configure Kafka listener to listen on all network interfaces.

### 4.0.3. Create a Kafka topic
### 4.0.4. Install/configure KafkaConnect for Azure IoT Hub

# 5.  Connect the dots - IoT Hub and Kafka



