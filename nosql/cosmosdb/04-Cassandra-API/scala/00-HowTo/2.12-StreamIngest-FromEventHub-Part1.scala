// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC A pre-requisite for this module is to complete the first notebook for "Bulk load from blob" that covers downloading and curating the public dataset.<BR>
// MAGIC   
// MAGIC This is part 1 of 4 notebooks that demonstrate stream ingest from Event Hub, with structured streaming & classic spark streaming, of 6.7 million records/1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In **this notebook**, we will publish curated Chicago crimes data to Event Hub for purpose of the exercise<BR>
// MAGIC - In notebook 2, we will (attempt to) ingest from Event Hub using structured stream processing and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 3, we will ingest from Event Hub using classic stream processing (DStream based) and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 4, we will ingest from Event Hub using structured stream processing and persist to a Databricks Delta table<BR>
// MAGIC   
// MAGIC With the four notebooks, we cover (publishing to Event Hub and then), reading from Event Hub in streaming mode and sinking to an OLTP store - the Azure Cosmos DB Cassandra table, and an analytics store - the Databricks Delta table.<BR>
// MAGIC Part 1 covers provisioning and configuring Event Hub for the exercise, also Azure Cosmos DB Cassandra API.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Pre-requisites (Azure Event Hub & Azure Cosmos DB Cassandra API)
// MAGIC Provision an Azure event hub, and provision an Azure Cosmos DB Cassandra instance

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.a. Provisioning
// MAGIC - **Azure Event Hub:** <br>
// MAGIC     https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create<br>
// MAGIC - **Azure Cosmos DB Cassandra API:** <br>
// MAGIC     https://docs.microsoft.com/en-us/azure/cosmos-db/create-cassandra-api-account-java#create-a-database-account

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.b. Specific to Azure Event Hub
// MAGIC 
// MAGIC ##### 1.0.1. Create an Azure Event Hub called crimes_aeh
// MAGIC Refer the provisioning documentation listed under 1.0.a
// MAGIC 
// MAGIC ##### 1.0.2. Create an Azure Event Hub consumer group 
// MAGIC Create an event hub instance for the event hub from 1.0.1 called crimes_chicago_cg
// MAGIC 
// MAGIC ##### 1.0.3. Create a SAS policy for the event hub with all checkboxes selected
// MAGIC This is covered in the link for provisioning in 1.0.a
// MAGIC 
// MAGIC ##### 1.0.4. From the portal, capture the connection string for the event hub
// MAGIC We will need this to write/read off of 1.0.1
// MAGIC 
// MAGIC ##### 1.0.5. Attach the Spark event hub connector to the Spark cluster
// MAGIC Refer documentation below to complete this step-<br>
// MAGIC https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html#requirements

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.b. Specific to Azure Cosmos DB Cassandra API
// MAGIC 
// MAGIC ##### 1.0.6. Create a keyspace from the portal - data explorer
// MAGIC Name: crimes_ks<br>
// MAGIC Throughput: 10,000<br>
// MAGIC 
// MAGIC ##### 1.0.7. Create a table from the portal - data explorer
// MAGIC Name: crimes_chicago_stream_aeh<br>
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

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.8. For the cqlsh savvy:
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
// MAGIC CREATE TABLE IF NOT EXISTS crimes_ks.crimes_chicago_stream_aeh(
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
// MAGIC You should see the keyspace you created.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 2.0. Publish to Azure Event Hub

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.1.  Define connection string & event hub configuration

// COMMAND ----------

import org.apache.spark.eventhubs._
//Replace connection string with your instances'.
val connectionString = "Endpoint=sb://crimes-ns.servicebus.windows.net/;SharedAccessKeyName=aeh_common;SharedAccessKey="   
val eventHubsConfWrite = EventHubsConf(connectionString)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2. Create a dataframe containing the source data

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType,BooleanType}

val crimesSchema = StructType(Array(
    StructField("case_id", StringType, true),
    StructField("case_nbr", StringType, true),
    StructField("case_dt_tm", StringType, true),
    StructField("block", StringType, true),
    StructField("iucr", StringType, true),
    StructField("primary_type", StringType, true),
    StructField("description", StringType, true),
    StructField("location_description", StringType, true),
    StructField("arrest_made", StringType, true),
    StructField("was_domestic", StringType, true),
    StructField("beat", StringType, true),
    StructField("district", StringType, true),
    StructField("ward", StringType, true),
    StructField("community_area", StringType, true),
    StructField("fbi_code", StringType, true),
    StructField("x_coordinate", StringType, true),
    StructField("y_coordinate", StringType, true),
    StructField("case_year", StringType, true),
    StructField("updated_dt", StringType, true),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true),
    StructField("location_coords", StringType, true),
    StructField("case_timestamp", TimestampType, true),
    StructField("case_month", IntegerType, true),
    StructField("case_day_of_month", IntegerType, true),
    StructField("case_hour", IntegerType, true),
    StructField("case_day_of_week_nbr", IntegerType, true),
    StructField("case_day_of_week_name", StringType, true)))

// COMMAND ----------

//val sourceDF = spark.sql("select * from crimes_db.chicago_crimes_curated")
val sourceDF = spark.readStream.schema(crimesSchema).load("/mnt/data/crimes/curatedDir/chicago-crimes-data")
sourceDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.3. Format the dataframe into an Event Hub compatible format
// MAGIC You will need to publish your data as a key-value pair of partition key and payload - the author has chosen JSON as format for the payload.

// COMMAND ----------

import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions._ 


val producerDF = sourceDF.select($"case_id" as "partitionKey", (to_json(struct(
$"case_id",$"case_nbr",$"case_dt_tm",$"block",$"iucr",$"primary_type",$"description",$"location_description",$"arrest_made",$"was_domestic",$"beat",$"district",$"ward",$"community_area",$"fbi_code",$"x_coordinate",$"y_coordinate",$"case_year",$"updated_dt",$"latitude",$"longitude",$"location_coords",$"case_timestamp",$"case_month",$"case_day_of_month",$"case_hour",$"case_day_of_week_nbr",$"case_day_of_week_name"))).cast(StringType) as "body")

// COMMAND ----------

producerDF.printSchema
//producerDF.show 

// COMMAND ----------

// MAGIC %md
// MAGIC #####  2.0.4. Publish to Azure Event Hub

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val dbfsCheckpointDirPath="/mnt/data/crimes/checkpointDir/chicago-crimes-stream-aeh-pub/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

//producerDF.writeStream.outputMode("append").format("console").trigger(ProcessingTime("5 seconds")).start().awaitTermination()

val query = producerDF
    .writeStream
    .format("eventhubs")
    .outputMode("update")
    .option("checkpointLocation", dbfsCheckpointDirPath)
    .options(eventHubsConfWrite.toMap)
    .trigger(ProcessingTime("2 seconds"))
    .start()