// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC A pre-requisite for this module is to complete the first notebook for "Bulk load from blob" that covers downloading and curating the public dataset.<BR>
// MAGIC   
// MAGIC This is part 1 of 4 notebooks that demonstrate stream ingest from Kafka, with structured streaming & classic/legacy spark streaming, of 6.7 million records/1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In **this notebook**, we will publish curated Chicago crimes data to Kafka for purpose of the exercise<BR>
// MAGIC - In notebook 2, we will (attempt to) ingest from Kafka using structured stream processing and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 3, we will ingest from Kafka using classic stream processing (DStream based) and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 4, we will ingest from Kafka using structured stream processing and persist to a Databricks Delta table<BR>
// MAGIC   
// MAGIC With the four notebooks, we cover (publishing to Kafka and then), reading from Kafka in streaming mode and sinking to an OLTP store - the Azure Cosmos DB Cassandra table, and an analytics store - the Databricks Delta table.<BR>
// MAGIC Part 1 covers provisioning and configuring HDInsight Kafka for the exercise, also Azure Cosmos DB Cassandra API.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Pre-requisites (Kafka & Azure Cosmos DB Cassandra API)
// MAGIC - Provision a Kafka cluster, and run through the process of creating a topic, setting up Vnet peering (Databricks Vnet and Kafka Vnet), Kafka IP advertising
// MAGIC - Provision a Cassandra API instance from the portal

// COMMAND ----------

// MAGIC %md
// MAGIC #### Provisioning
// MAGIC - HDInsight Kafka: https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-get-started
// MAGIC - Azure Cosmos DB Cassandra API: https://docs.microsoft.com/en-us/azure/cosmos-db/create-cassandra-api-account-java#create-a-database-account

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.a. Specific to HDInsight Kafka - to be run on the Kafka cluster, Linux CLI
// MAGIC 
// MAGIC ##### 1.0.1. Get the zookeeper server list for the cluster
// MAGIC 
// MAGIC Run this on the terminal of the headnode of your Kafka cluster to get the zookeeper server list with port number.<br>
// MAGIC This is required for creating a Kafka topic in your Kafka cluster from the CLI.
// MAGIC ```Scala
// MAGIC CLUSTERNAME="YOUR_CLUSTERNAME"
// MAGIC ZOOKEEPER_HOSTS=`curl -u admin -G "https://$CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$CLUSTERNAME/services/ZOOKEEPER/components/ZOOKEEPER_SERVER" | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2`
// MAGIC ```
// MAGIC After the command completes, run this-
// MAGIC ```Scala
// MAGIC echo $ZOOKEEPER_HOSTS
// MAGIC ```
// MAGIC ##### 1.0.2. Get the broker list for the cluster
// MAGIC 
// MAGIC Run this on the terminal of the headnode of your Kafka cluster to get the broker list with port number.<br>
// MAGIC This is required for smoke testing your Kafka cluster with the Kafka console producer and consumer, from the CLI of your Kafka cluster.
// MAGIC ```Scala
// MAGIC KAFKA_BROKERS=`curl -u admin -G "https://$CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$CLUSTERNAME/services/KAFKA/components/KAFKA_BROKER" | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2`
// MAGIC ```
// MAGIC After the command completes, run this-
// MAGIC ```Scala
// MAGIC echo $KAFKA_BROKERS
// MAGIC ```
// MAGIC ##### 1.0.3. Create a Kafka topic called crimes_chicago
// MAGIC Run this on the terminal of the headnode of your Kafka cluster - 
// MAGIC ```
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 8 --topic crimes_chicago --zookeeper $ZOOKEEPER_HOSTS
// MAGIC ```
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
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list $KAFKA_BROKERS --topic test_topic
// MAGIC ```
// MAGIC Type anything as test messages after the > prompt appears.
// MAGIC 
// MAGIC 4.  Launch the Kafka console consumer in another terminal window to validate if you can see what you typed in 3.
// MAGIC Run the command in 1.0.2 to initialize the Kafka broker list to the variable KAFKA_BROKERS.<BR>  
// MAGIC Then run the command below to launch the Kafka console consumer.
// MAGIC ```
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKERS --topic test_topic --from-beginning
// MAGIC ```
// MAGIC 5.  Delete the test topic from 1.0.4, step 1 to close out the smoke test from any of the two terminals open on the HDInsight Kafka cluster
// MAGIC ```
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper $ZOOKEEPER_HOSTS --delete  --topic test_topic
// MAGIC ```
// MAGIC 
// MAGIC ##### 1.0.5.  Get the broker private IP list for use in Spark
// MAGIC From databricks, you cannot use the broker names from the steps above, they will not resolve, instead you need the private IPs.<BR>
// MAGIC Go to the Ambari hosts page and get the broker private IPs.  The broker names start with wn (for worker node).<br>
// MAGIC For example, for the author, they were- 10.7.0.12,10.7.0.13, 10.7.0.14, 10.7.0.15<br>
// MAGIC The port number is 9092; So the author's borker list for use in Spark was - "10.7.0.12:9092, 10.7.0.13:9092,10.7.0.14:9092,10.7.0.15:9092"
// MAGIC   

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.b. Specific to HDInsight Kafka - to be run on Ambari, on the Kafka cluster
// MAGIC 
// MAGIC ##### 1.0.6. Vnet peering (Kafka and Databricks) to enable communication with private IPs
// MAGIC 
// MAGIC 1.  Go to the Databricks workspace on the portal and configure peering with HDInsight Kafka cluster's Vnet
// MAGIC 2.  Go to the HDInsight Kafka cluster's Vnet on the portal and peer to the Databricks workers Vnet.
// MAGIC Without this peering, you will not be able to work with Kafka from Databricks.<BR>
// MAGIC 
// MAGIC **Your Kafka version is** - go to Ambari - admin -> stack and versions and you will find the Kafka version

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
// MAGIC #### 1.0.e. Specific to Azure Cosmos DB Cassandra API
// MAGIC 
// MAGIC ##### 1.0.9. Create a keyspace from the data explorer on the portal 
// MAGIC Name: crimes_ks<br>
// MAGIC 
// MAGIC ##### 1.0.10a. Create a table from the data explorer on the portal
// MAGIC Name: crimes_chicago_stream_kafka<br>
// MAGIC Keyspace: crimes_ks<br>
// MAGIC Throughput: 10,000<br>
// MAGIC Columns:<br>
// MAGIC ```
// MAGIC (
// MAGIC case_id int primary key,
// MAGIC case_nbr text,
// MAGIC case_dt_tm text,
// MAGIC block text,
// MAGIC iucr text,
// MAGIC primary_type text,
// MAGIC description text,
// MAGIC location_description text,
// MAGIC arrest_made boolean,
// MAGIC was_domestic boolean,
// MAGIC beat int,
// MAGIC district int,
// MAGIC ward int,
// MAGIC community_area int,
// MAGIC fbi_code text,
// MAGIC x_coordinate int,
// MAGIC y_coordinate int,
// MAGIC case_year int,
// MAGIC updated_dt text,
// MAGIC latitude double,
// MAGIC longitude double,
// MAGIC location_coords text,
// MAGIC case_timestamp timestamp,
// MAGIC case_month int,
// MAGIC case_day_of_month int,
// MAGIC case_hour int,
// MAGIC case_day_of_week_nbr int,
// MAGIC case_day_of_week_name text)
// MAGIC ```
// MAGIC 
// MAGIC 
// MAGIC ##### 1.0.10b. Scale the throughput of the table to 50,000 RUs from the data explorer on the portal
// MAGIC This needs to be a separate process and cannot be rolled into the previous step.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.11. For the cqlsh savvy:
// MAGIC 
// MAGIC **To connect to CosmosDB Cassandra API:**
// MAGIC ```
// MAGIC export SSL_VERSION=TLSv1_2
// MAGIC export SSL_VALIDATE=false
// MAGIC cqlsh.py YOUR_ACCOUNT_NAME.cassandra.cosmosdb.azure.com 10350 -u YOUR_ACCOUNT_NAME -p YOUR_ACCOUNT_PASSWORD --ssl
// MAGIC ```
// MAGIC 
// MAGIC **To create the keyspace:**
// MAGIC ```
// MAGIC CREATE KEYSPACE IF NOT EXISTS crimes_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }; 
// MAGIC ```
// MAGIC 
// MAGIC **To validate keyspace creation:**
// MAGIC ```
// MAGIC DESCRIBE keyspaces;
// MAGIC ```
// MAGIC You should see the keyspace you created.
// MAGIC 
// MAGIC **To create the table:**
// MAGIC ```
// MAGIC CREATE TABLE IF NOT EXISTS crimes_ks.crimes_chicago_stream(
// MAGIC case_id int primary key,
// MAGIC case_nbr text,
// MAGIC case_dt_tm text,
// MAGIC block text,
// MAGIC iucr text,
// MAGIC primary_type text,
// MAGIC description text,
// MAGIC location_description text,
// MAGIC arrest_made boolean,
// MAGIC was_domestic boolean,
// MAGIC beat int,
// MAGIC district int,
// MAGIC ward int,
// MAGIC community_area int,
// MAGIC fbi_code text,
// MAGIC x_coordinate int,
// MAGIC y_coordinate int,
// MAGIC case_year int,
// MAGIC updated_dt text,
// MAGIC latitude double,
// MAGIC longitude double,
// MAGIC location_coords text,
// MAGIC case_timestamp timestamp,
// MAGIC case_month int,
// MAGIC case_day_of_month int,
// MAGIC case_hour int,
// MAGIC case_day_of_week_nbr int,
// MAGIC case_day_of_week_name text)
// MAGIC WITH cosmosdb_provisioned_throughput=10000;
// MAGIC ```
// MAGIC 
// MAGIC **To validate table creation:**
// MAGIC ```
// MAGIC USE crimes_ks;
// MAGIC DESCRIBE tables;
// MAGIC ```
// MAGIC You should see the keyspace you created. Dont forget to scale the throughput.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 2.0. Publish to Kafka

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.1.  Define topic and broker list
// MAGIC Note: If you are using Azure HDInsight Kafka, the broker names will not work, you need the IP addresses.

// COMMAND ----------

val kafkaTopic = "crimes_chicago"
val kafkaBrokerAndPortCSV = "10.7.0.12:9092,10.7.0.13:9092,10.7.0.14:9092,10.7.0.15:9092"

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2. Create a dataframe containing the source data

// COMMAND ----------

val sourceDF = spark.sql("SELECT * FROM crimes_db.chicago_crimes_curated where district = '007'")
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

// COMMAND ----------

