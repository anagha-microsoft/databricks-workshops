// Databricks notebook source
// MAGIC %md 
// MAGIC # What's in this notebook?
// MAGIC We will publish taxi trips in DBFS backed by Azure Blob Storage to Azure Event Hub to create a streaming source for the lab<br>
// MAGIC 
// MAGIC In this tutorial, we will:
// MAGIC 1.  Format taxi trip data to be compatible with Azure Event Hub
// MAGIC 2.  Publish to Azure Event Hub

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### About publishing events to an Azure Event Hub
// MAGIC 
// MAGIC Azure Event Hub expects the data published to be of the following format.
// MAGIC 
// MAGIC **Column and type** 
// MAGIC 1. body 	                 - *binary*
// MAGIC 2. partition 	             - *string*
// MAGIC 3. offset                    - *string*
// MAGIC 4. sequenceNumber            - *long*
// MAGIC 5. enqueuedTime              - *timestamp*
// MAGIC 6. publisher                 - *string*
// MAGIC 7. partitionKey              - *string*
// MAGIC 8. properties 	             - *map[string,json]*
// MAGIC 
// MAGIC The body is always provided as a byte array.
// MAGIC 
// MAGIC **The body column is the only required option**. If a partitionId and partitionKey are not provided, then events will distributed to partitions using a round-robin model.
// MAGIC Users can also provided properties via a map[string,json] if they would like to send any additional properties with their events.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Common variables

// COMMAND ----------

// MAGIC %run ./01-common

// COMMAND ----------

// MAGIC %md ### 2.0. Format dataset to be Azure Event Hub compatible

// COMMAND ----------

// 1.  Define schema
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType,BooleanType, DecimalType}

val tripSchema = StructType(Array(
    StructField("vendor_id", StringType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", FloatType, true),
    StructField("rate_code_id", StringType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("pickup_locn_id", StringType, true),
    StructField("dropoff_locn_id", StringType, true),
    StructField("payment_type", StringType, true),
    StructField("fare_amount", FloatType, true),
    StructField("extra", FloatType, true),
    StructField("mta_tax", FloatType, true),
    StructField("tip_amount", FloatType, true),
    StructField("tolls_amount", FloatType, true),
    StructField("improvement_surcharge", FloatType, true),
    StructField("total_amount", DoubleType, true)
))

// COMMAND ----------

// 2. Read raw
val yellowTaxiTripsStreamingDF = spark.readStream
                      .schema(tripSchema)
                      .load("/mnt/workshop/raw/transactional-data/trips/")

// COMMAND ----------

import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType,BooleanType, DecimalType}
import org.apache.spark.sql.functions._ 

val producerDF = yellowTaxiTripsStreamingDF.select((to_json(struct(
$"vendor_id",$"pickup_datetime",$"dropoff_datetime",$"passenger_count",$"trip_distance",$"rate_code_id",$"store_and_fwd_flag",$"pickup_locn_id",$"dropoff_locn_id",$"payment_type",$"fare_amount",$"extra",$"mta_tax",$"tip_amount",$"tolls_amount",$"improvement_surcharge",$"total_amount"))).cast(StringType) as "body")

// COMMAND ----------

producerDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC We have not provided a partition key so the distribution across Event Hub partitions is round-robin.

// COMMAND ----------

// MAGIC %md ### 4.0. Publish to Azure Event Hub

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Create a checkpoint directory 
val dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-producer/"
//  dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

// COMMAND ----------

// For testing - stream to console
//producerDF.writeStream.outputMode("append").format("console").trigger(ProcessingTime("2 seconds")).start().awaitTermination()

// COMMAND ----------

// Publish stream to Azure Event Hub

import org.apache.spark.eventhubs._

val eventHubsConf = EventHubsConf(aehConexionCreds)
val query = producerDF
    .writeStream
    .format("eventhubs")
    .outputMode("update")
    .option("checkpointLocation", dbfsCheckpointDirPath)
    .options(eventHubsConf.toMap)
    .trigger(ProcessingTime("2 seconds"))
    .start()


// COMMAND ----------

// MAGIC %md
// MAGIC **Keep this notebook running for the rest of the lab.**  Proceed to the next notebook in a separate browser window.