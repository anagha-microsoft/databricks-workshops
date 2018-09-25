// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 1 of 2 notebooks that demonstrate bulk load from Kafka into Azure Cosmos DB Cassandra API - of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC A pre-requisite for this module is to complete the first notebook for "Bulk load from blob" that covers downloading and curating the public dataset.<BR>
// MAGIC 
// MAGIC In this notebook, we will publish the curated crime data to a Kafka topic as part of a batch process.  This will set the stage for the next notebook that covers bulk loading from Kafka in batch into Azure Cosmos DB Cassandra API.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Pre-requisites
// MAGIC Provision a Kafka cluster and run through the process of creating a topic, setting up Vnet peering (Databricks Vnet and Kafka Vnet), Kafka IP advertising

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.a. Specific to HDInsight Kafka - to be run on the Kafka cluster, Linux CLI
// MAGIC 
// MAGIC ##### 1.0.1. Get the zookeeper server list for the cluster
// MAGIC 
// MAGIC Run this on the terminal of the headnode of your Kafka cluster to get the zookeeper server list with port number-
// MAGIC ```Scala
// MAGIC CLUSTERNAME="YOUR_CLUSTERNAME"
// MAGIC PASSWORD="YOUR_CLUSTERPASSWORD"
// MAGIC curl -u admin:$YOUR_CLUSTERPASSWORD -G "https://$YOUR_CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$YOUR_CLUSTERNAME/services/ZOOKEEPER/components/ZOOKEEPER_SERVER" | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2
// MAGIC ```
// MAGIC 
// MAGIC ##### 1.0.2. Get the broker list for the cluster
// MAGIC 
// MAGIC Run this on the terminal of the headnode of your Kafka cluster to get the broker list with port number-
// MAGIC ```Scala
// MAGIC CLUSTERNAME="YOUR_CLUSTERNAME"
// MAGIC export KAFKABROKERS=`curl -u admin -G "https://$YOUR_CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$YOUR_CLUSTERNAME/services/KAFKA/components/KAFKA_BROKER" | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2`
// MAGIC ```
// MAGIC 
// MAGIC ##### 1.0.3. Create a Kafka topic called crimes_chicago_topic
// MAGIC Run this on the terminal of the headnode of your Kafka cluster - 
// MAGIC ```
// MAGIC ZOOKEEPER_HOSTS=YOUR_ZOOKEEPER_HOSTS_FROM_1.0.1
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 8 --topic crimes_chicago_topic --zookeeper $ZOOKEEPER_HOSTS
// MAGIC ```
// MAGIC For example:
// MAGIC ```
// MAGIC ZOOKEEPER_HOSTS="zk0-gaia-k.fy0cecrwzco...cx.internal.cloudapp.net:2181,zk1-gaia-k.fy0cecrwzcoe...cx.internal.cloudapp.net:2181,zk2-gaia-k.fy0cec...cx.internal.cloudapp.net"
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 8 --topic crimes_chicago_topic --zookeeper $ZOOKEEPER_HOSTS
// MAGIC Created topic "crimes_chicago_topic".
// MAGIC ```
// MAGIC 
// MAGIC ##### 1.0.4. Smoke test of your Kafka cluster using Kafka utilities - console producer and consumer
// MAGIC 
// MAGIC 1.  Create test topic
// MAGIC ```
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 2 --topic test_topic --zookeeper $ZOOKEEPER_HOSTS
// MAGIC ```
// MAGIC 2.  List topics
// MAGIC ```
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER_HOSTS
// MAGIC ```
// MAGIC 3.  Launch the Kafka console producer in one window & type into it
// MAGIC ```
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list $KAFKABROKERS --topic test_topic
// MAGIC ```
// MAGIC Type anything as test messages after the > prompt appears.
// MAGIC 
// MAGIC 4.  Launch the Kafka console consumer in another terminal window to validate if you can see what you typed in 3.
// MAGIC Run the command in 1.0.2 to initialize the Kafka broker list to the variable KAFKABROKERS.<BR>  
// MAGIC Then run the command below to launch the Kafka console consumer.
// MAGIC ```
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --bootstrap-server $KAFKABROKERS --topic test_topic --from-beginning
// MAGIC ```
// MAGIC 5.  Delete the test topic from 1.0.4, step 1 to close out the smoke test from any of the two terminals open on the HDInsight Kafka cluster
// MAGIC ```
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper $ZOOKEEPER_HOSTS --delete  --topic test_topic
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.b. Specific to HDInsight Kafka - to be run on Ambari, on the Kafka cluster
// MAGIC 
// MAGIC ##### 1.0.6. Vnet peering (Kafka and Databricks) to enable communication with private IPs
// MAGIC 
// MAGIC 1.  Go to the Databricks workspace on the portal and configure peering with HDInsight Kafka cluster's Vnet
// MAGIC 2.  Go to the HDInsight Kafka cluster's Vnet on the portal and peer to the Databricks workers Vnet.
// MAGIC Without this peering, you will not be able to work with Kafka from Databricks.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.c. Specific to HDInsight Kafka - to be run on Azure portal
// MAGIC 
// MAGIC ##### 1.0.7. Configure Kafka for IP advertising
// MAGIC Kafka needs to be configured for IP advertising - there are two steps to be completed followed by a cluster restart.<BR>
// MAGIC https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-connect-vpn-gateway#configure-kafka-for-ip-advertising

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.d. Specific to Kafka in general - to be run on Azure Databricks from the portal
// MAGIC ##### 1.0.8. Add the Spark Kafka library to the cluster
// MAGIC Find the compatible version on Maven central.  For HDInsight 3.6, with Kafka 1.1/1.0/0.10.1, and Databricks Runtime 4.3, Spark 2.3.1, Scala 2.11, the author used-<br>
// MAGIC org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 2.0. Publish to Kafka

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.1.  Define topic and broker list
// MAGIC Note: If you are using Azure HDInsight Kafka, the broker names will not work, you need the IP addresses.

// COMMAND ----------

val kafkaTopic = "crimes_chicago_topic"
val kafkaBrokerAndPortCSV = "10.7.0.4:9092, 10.7.0.5:9092,10.7.0.8:9092,10.7.0.12:9092"

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2. Create a dataframe containing the source data

// COMMAND ----------

val sourceDF = spark.sql("SELECT * FROM crimes_db.chicago_crimes_curated")
sourceDF.printSchema
sourceDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.3. Format the dataframe into a Kafka compatible format
// MAGIC You will need to publish your data as a key value pair.

// COMMAND ----------

import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions._ 

val producerDF = sourceDF.select($"case_id" as "key", (to_json(struct(
$"case_id",$"case_nbr",$"case_dt_tm",$"block",$"iucr",$"primary_type",$"description",$"location_description",$"arrest_made",$"was_domestic",$"beat",$"district",$"ward",$"community_area",$"fbi_code",$"x_coordinate",$"y_coordinate",$"case_year",$"updated_dt",$"latitude",$"longitude",$"location_coords",$"case_timestamp",$"case_month",$"case_day_of_month",$"case_hour",$"case_day_of_week_nbr",$"case_day_of_week_name"))) as "value")

// COMMAND ----------

producerDF.printSchema
producerDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC #####  2.0.4. Publish to Kafka

// COMMAND ----------

// Publish to kafka - took 38 seconds
producerDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokerAndPortCSV)
  .option("topic", kafkaTopic)
  .save