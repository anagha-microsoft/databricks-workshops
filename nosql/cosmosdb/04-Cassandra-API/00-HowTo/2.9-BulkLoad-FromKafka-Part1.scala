// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 1 or 2 notebooks that demonstrate bulk load from Kafka into Azure Cosmos DB Cassandra API - of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC A pre-requisite for this module is to complete the first notebook for "Bulk load from blob" that covers downloading and curating the public dataset.<BR>
// MAGIC 
// MAGIC In this notebook, we will publish the curated crime data to a Kafka topic as part of a batch process.  This will set the stage for the next notebook that covers bulk loading from Kafka in batch into Azure Cosmos DB Cassandra API.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Pre-requisites
// MAGIC 
// MAGIC ##### 1.0.1. Create a Kafka topic on your Kafka cluster
// MAGIC On HDInsight Kafka, the following is the command to create a topic - 
// MAGIC /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 8 --topic crimes_chicago_topic --zookeeper $YOUR_ZOOKEEPER_HOSTS
// MAGIC 
// MAGIC ```
// MAGIC akhanolk@hn0-gaia-k:~$ ZOOKEEPER_HOSTS="zk0-gaia-k.fy0cecrwzco...cx.internal.cloudapp.net:2181,zk1-gaia-k.fy0cecrwzcoe...cx.internal.cloudapp.net:2181,zk2-gaia-k.fy0cec...cx.internal.cloudapp.net"
// MAGIC akhanolk@hn0-gaia-k:~$ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 8 --topic crimes_chicago_topic --zookeeper $ZOOKEEPER_HOSTS
// MAGIC Created topic "crimes_chicago_topic".
// MAGIC ```

// COMMAND ----------

val kafkaTopic = "crimes_chicago_topic"

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.2. Get your list of brokers into a variable
// MAGIC Note: If you are using Azure HDInsight Kafka, the broker names will not work, you need the IP addresses.

// COMMAND ----------

val kafkaBrokerAndPortCSV = "10.7.0.4:9092, 10.7.0.5:9092,10.7.0.8:9092,10.7.0.12:9092"

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.3. Add the Spark Kafka library to the cluster
// MAGIC Find the compatible version on Maven central.  For HDInsight 3.6, with Kafka 1.1/1.0/0.10.1, and Databricks Runtime 4.3, Spark 2.3.1, Scala 2.11, the author used-<br>
// MAGIC org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Create a dataframe containing the source data

// COMMAND ----------

val sourceDF = spark.sql("SELECT * FROM crimes_db.chicago_crimes_curated")
sourceDF.printSchema
sourceDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Format the dataframe into a Kafka compatible format
// MAGIC You will need to publish your data as a key value pair.

// COMMAND ----------

import org.apache.spark.sql.functions.to_json
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.sql.functions._ 

val producerDF = sourceDF.select($"case_id" as "key", (to_json(struct(
$"case_id",$"case_nbr",$"case_dt_tm",$"block",$"iucr",$"primary_type",$"description",$"location_description",$"arrest_made",$"was_domestic",$"beat",$"district",$"ward",$"community_area",$"fbi_code",$"x_coordinate",$"y_coordinate",$"case_year",$"updated_dt",$"latitude",$"longitude",$"location_coords",$"case_timestamp",$"case_month",$"case_day_of_month",$"case_hour",$"case_day_of_week_nbr",$"case_day_of_week_name"))) as "value")

// COMMAND ----------

producerDF.printSchema
producerDF.show

// COMMAND ----------

// Publish to kafka
producerDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokerAndPortCSV)
  .option("topic", kafkaTopic)
  .save()