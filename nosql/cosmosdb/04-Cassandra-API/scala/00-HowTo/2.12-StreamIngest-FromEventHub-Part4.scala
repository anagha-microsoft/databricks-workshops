// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 4 of 4 notebooks that demonstrate stream ingest from Azure Event Hub, of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In notebook 1, we published data to Azure Event Hub for purpose of the exercise<BR>
// MAGIC - In notebook 2, we attempted to ingest from Azure Event Hub using structured stream processing and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 3, we ingest from Azure Event Hub using classic stream processing (DStream based) and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In **this notebook**, we will ingest from Azure Event Hub using structured stream processing and persist to a Databricks Delta table<BR>
// MAGIC   
// MAGIC While the Azure Cosmos DB Cassandra table serves as a hot store for OLTP, the Delta table will serve as an analytics store.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read stream from Azure Event Hub

// COMMAND ----------

import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._

// 1) AEH checkpoint related
val dbfsCheckpointDirPath="/mnt/data/crimes/checkpointDir/chicago-crimes-stream-aeh-consumer/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

// 2) AEH consumer related

//Replace connection string with your instances'.
val aehConsumerConnString = "Endpoint=sb://crimes-ns.servicebus.windows.net/;SharedAccessKeyName=aeh_common;SharedAccessKey=" 
val aehConsumerParams =
  EventHubsConf(aehConsumerConnString)
  .setConsumerGroup("crimes_chicago_cg")
  .setStartingPosition(EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(1000)

// 3) Consume from AEH
val streamingDF = spark.readStream.format("eventhubs").options(aehConsumerParams.toMap).load()

val partParsedStreamDF =
  streamingDF
  .withColumn("aeh_offset", $"offset".cast(LongType))
  .withColumn("time_readable", $"enqueuedTime".cast(TimestampType))
  .withColumn("time_as_is", $"enqueuedTime".cast(LongType))
  .withColumn("json_payload", $"body".cast(StringType))
  .select("aeh_offset", "time_readable", "time_as_is", "json_payload")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Parse events from Azure Event Hub

// COMMAND ----------

import org.apache.spark.sql.functions._

val consumableDF = partParsedStreamDF.select(get_json_object($"json_payload", "$.case_id").cast(IntegerType).alias("case_id"),
                                          get_json_object($"json_payload", "$.case_nbr").alias("case_nbr"),
                                          get_json_object($"json_payload", "$.case_dt_tm").alias("case_dt_tm"),
                                          get_json_object($"json_payload", "$.block").alias("block"),
                                          get_json_object($"json_payload", "$.iucr").alias("iucr"),
                                          get_json_object($"json_payload", "$.primary_type").alias("primary_type"),
                                          get_json_object($"json_payload", "$.description").alias("description"),
                                          get_json_object($"json_payload", "$.location_description").alias("location_description"),
                                          get_json_object($"json_payload", "$.arrest_made").cast(BooleanType).alias("arrest_made"),
                                          get_json_object($"json_payload", "$.was_domestic").cast(BooleanType).alias("was_domestic"),
                                          get_json_object($"json_payload", "$.beat").cast(IntegerType).alias("beat"),
                                          get_json_object($"json_payload", "$.district").cast(IntegerType).alias("district"),
                                          get_json_object($"json_payload", "$.ward").cast(IntegerType).alias("ward"),
                                          get_json_object($"json_payload", "$.fbi_code").alias("fbi_code"),
                                          get_json_object($"json_payload", "$.x_coordinate").cast(IntegerType).alias("x_coordinate"),
                                          get_json_object($"json_payload", "$.y_coordinate").cast(IntegerType).alias("y_coordinate"),
                                          get_json_object($"json_payload", "$.case_year").cast(IntegerType).alias("case_year"),
                                          get_json_object($"json_payload", "$.updated_dt").alias("updated_dt"),
                                          get_json_object($"json_payload", "$.latitude").cast(DoubleType).alias("latitude"),
                                          get_json_object($"json_payload", "$.longitude").cast(DoubleType).alias("longitude"),
                                          get_json_object($"json_payload", "$.location_coords").alias("location_coords"),
                                          get_json_object($"json_payload", "$.case_timestamp").cast(TimestampType).alias("case_timestamp"),
                                          get_json_object($"json_payload", "$.case_month").cast(IntegerType).alias("case_month"),
                                          get_json_object($"json_payload", "$.case_day_of_month").cast(IntegerType).alias("case_day_of_month"),          
                                          get_json_object($"json_payload", "$.case_hour").cast(IntegerType).alias("case_hour"),
                                          get_json_object($"json_payload", "$.case_day_of_week_nbr").cast(IntegerType).alias("case_day_of_week_nbr"),
                                          get_json_object($"json_payload", "$.case_day_of_week_name").alias("case_day_of_week_name"))

// COMMAND ----------

//Console output of parsed events
//consumableDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

consumableDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Sink to Databricks Delta

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 3.0.1. Sink to delta table

// COMMAND ----------

//1) Destination directory for delta table, and for checkpoints
val dbfsDestDirPath="/mnt/data/crimes/curatedDir/chicago-crimes-stream-delta-aeh/"
val dbfsCheckpointDirPath="/mnt/data/crimes/checkpointDir/chicago-crimes-stream-delta-aeh/"

//2) Remove output from prior execution
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

//3) Persist as delta format (Parquet) to curated zone to a delta table
consumableDF.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", dbfsCheckpointDirPath)
  .start(dbfsDestDirPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 3.0.2. Read from delta table
// MAGIC Run the below in a different notebook while the stream process is running;  Alternately, you can shut down the streaming process and try this out.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS crimes_db;
// MAGIC 
// MAGIC USE crimes_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS chicago_crimes_delta_stream_aeh;
// MAGIC CREATE TABLE chicago_crimes_delta_stream_aeh
// MAGIC USING DELTA
// MAGIC LOCATION '/mnt/data/crimes/curatedDir/chicago-crimes-stream-delta-aeh';
// MAGIC 
// MAGIC OPTIMIZE chicago_crimes_delta_stream_aeh;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from crimes_db.chicago_crimes_delta_stream_aeh;