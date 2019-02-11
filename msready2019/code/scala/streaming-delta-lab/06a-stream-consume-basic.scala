// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this notebook?
// MAGIC 1.  Learn to consume streaming data from Azure Event Hub and publish to Databricks Delta

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.0. Read stream

// COMMAND ----------

// MAGIC %run ./01-common

// COMMAND ----------

import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._

// 1) AEH consumer related
val aehConsumerParams =
  EventHubsConf(aehConexionCreds)
  .setConsumerGroup("nyc-aeh-topic-streaming-cg")
  .setStartingPosition(EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(500)

// COMMAND ----------

// 2) Read the stream 
val incomingStreamingDF = spark.readStream.format("eventhubs").options(aehConsumerParams.toMap).load()
incomingStreamingDF.printSchema
    
// 3) Body is binary, so we cast it to string to see the actual content of the message
val tripsOnlyStreamingDF = incomingStreamingDF.withColumn("json_payload", $"body".cast(StringType))

// 4) Parse events
import org.apache.spark.sql.functions._

val consumableStreamingDF = tripsOnlyStreamingDF.select(get_json_object($"json_payload", "$.vendor_id").alias("vendor_id"),
                                          get_json_object($"json_payload", "$.pickup_datetime").cast(TimestampType).alias("pickup_datetime"),
                                          get_json_object($"json_payload", "$.dropoff_datetime").cast(TimestampType).alias("dropoff_datetime"),
                                          get_json_object($"json_payload", "$.passenger_count").alias("passenger_count"),
                                          get_json_object($"json_payload", "$.trip_distance").alias("trip_distance"),
                                          get_json_object($"json_payload", "$.rate_code_id").alias("rate_code_id"),
                                          get_json_object($"json_payload", "$.store_and_fwd_flag").alias("store_and_fwd_flag"),
                                          get_json_object($"json_payload", "$.pickup_locn_id").alias("pickup_locn_id"),
                                          get_json_object($"json_payload", "$.dropoff_locn_id").alias("dropoff_locn_id"),
                                          get_json_object($"json_payload", "$.payment_type").alias("payment_type"),
                                          get_json_object($"json_payload", "$.fare_amount").cast(FloatType).alias("fare_amount"),
                                          get_json_object($"json_payload", "$.extra").cast(FloatType).alias("extra"),
                                          get_json_object($"json_payload", "$.mta_tax").cast(FloatType).alias("mta_tax"),
                                          get_json_object($"json_payload", "$.tip_amount").cast(FloatType).alias("tip_amount"),
                                          get_json_object($"json_payload", "$.tolls_amount").cast(FloatType).alias("tolls_amount"),
                                          get_json_object($"json_payload", "$.improvement_surcharge").cast(FloatType).alias("improvement_surcharge"),
                                          get_json_object($"json_payload", "$.total_amount").cast(FloatType).alias("total_amount"))

consumableStreamingDF.printSchema

// COMMAND ----------

// 5) Console output of parsed events for testing only
//consumableStreamingDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember to:** Cancel the command (command 6) above once you see a table displayed with results and see it updating.  We will need our cluster resources available for subsequent steps.

// COMMAND ----------

// MAGIC %md **Note:** Event Hubs has a small internal buffer (~256kB by default). Due to this reason, the incoming stream for successive queries might be slow or take some time to load. Please wait for a couple of minutes after firing your query to see the results appear. 

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember to:** Cancel the command above once you see a table displayed with results and see it updating.  We will need our cluster resources available for subsequent steps.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.0. Persist stream to Databricks Delta

// COMMAND ----------

//1.  Destination directory for delta table, and for checkpoints
val dbfsDestDirPath="/mnt/workshop/curated/transactions/trips"
val dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-aeh-sub/"

//Remove output from prior execution
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

//Persist as delta format (Parquet) to curated zone to a delta table
consumableStreamingDF.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", dbfsCheckpointDirPath)
  .start(dbfsDestDirPath)

// COMMAND ----------

// MAGIC %md
// MAGIC **Let this run for 5 minutes:** Proceed to the next notebook to learn to query the Delta table in a separate browser window.