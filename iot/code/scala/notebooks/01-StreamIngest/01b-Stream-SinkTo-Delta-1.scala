// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 2 of 3 notebooks that demonstrate stream ingest from Kafka, of live telemetry from Azure IoT hub.<BR>
// MAGIC - In the notebook, 1, we ingested from Kafka into Azure Cosmos DB SQL API (OLTP store)<BR>
// MAGIC - In **this notebook**, 2, we will ingest from Kafka using structured stream processing and persist to a Databricks Delta table (analytics store)<BR>
// MAGIC - In the next notebook, 3, we will run queries against the Delta table<BR>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read stream from Kafka

// COMMAND ----------

val kafkaTopic = "iot_telemetry_in"
//Replace with your Kafka broker IPs
val kafkaBrokerAndPortCSV = "10.1.0.11:9092, 10.1.0.12:9092,10.1.0.13:9092,10.1.0.14:9092"

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val kafkaSourceDF = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokerAndPortCSV)
  .option("subscribe", kafkaTopic)
  .option("startingOffsets", "earliest")
  .load()

val rawDF = kafkaSourceDF.selectExpr("CAST(value AS STRING) as iot_payload").as[(String)]

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Parse events from kafka

// COMMAND ----------

case class TelemetryRow(device_id: String, 
                        telemetry_ts: String, 
                        temperature: Double, 
                        temperature_unit: String, 
                        humidity: Double, 
                        humidity_unit: String, 
                        pressure: Double, 
                        pressure_unit: String,
                        telemetry_year: String,
                        telemetry_month:String,
                        telemetry_day:String,
                        telemetry_hour: String,
                        telemetry_minute: String
                       )

// COMMAND ----------

def parseDeviceTelemetry(payloadString: String): (String,String,String,String,String,String,String,String) = {
  val device_id=payloadString.substring(payloadString.indexOf("=")+1,payloadString.indexOf(","))
  val telemetry_ts=payloadString.substring(payloadString.indexOf("enqueuedTime=")+13,payloadString.indexOf(",sequenceNumber="))
  val telemetry_json=payloadString.substring(payloadString.indexOf(",content=")+9,payloadString.indexOf(",systemProperties="))
  //Return tuple
  (
     device_id,
     telemetry_ts,
     telemetry_json,
     telemetry_ts.substring(0,4),//telemetry_year
     telemetry_ts.substring(5,7),//telemetry_month
     telemetry_ts.substring(8,10),//telemetry_day
     telemetry_ts.substring(11,13),//telemetry_hour
     telemetry_ts.substring(14,16)//telemetry_minute
  )
}

// COMMAND ----------

val parsedDF=rawDF.map(x => parseDeviceTelemetry(x)).toDF("device_id","telemetry_ts","telemetry_json","telemetry_year","telemetry_month","telemetry_day","telemetry_hour","telemetry_minute")
val telemetryDF = parsedDF.select($"device_id", 
                                  $"telemetry_ts", 
                                  get_json_object($"telemetry_json", "$.temperature").cast(DoubleType).alias("temperature"),
                                  get_json_object($"telemetry_json", "$.temperature_unit").alias("temperature_unit"),
                                  get_json_object($"telemetry_json", "$.humidity").cast(DoubleType).alias("humidity"),
                                  get_json_object($"telemetry_json", "$.humidity_unit").alias("humidity_unit"),
                                  get_json_object($"telemetry_json", "$.pressure").cast(DoubleType).alias("pressure"),
                                  get_json_object($"telemetry_json", "$.pressure_unit").alias("pressure_unit"),
                                  $"telemetry_year",
                                  $"telemetry_month",
                                  $"telemetry_day",
                                  $"telemetry_hour",
                                  $"telemetry_minute").as[TelemetryRow]

// COMMAND ----------

//For testing purposes only
//telemetryDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Sink to Databricks Delta

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 3.0.1. Sink to delta table

// COMMAND ----------

//1) Destination directory for delta table, and for checkpoints
val dbfsDestDirPath="/mnt/data/iot/rawDir/telemetry/"
val dbfsCheckpointDirPath="/mnt/data/iot/checkpointDir/telemetry-delta/"

//2) Remove output from prior execution
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

//3) Persist as delta format (Parquet) to curated zone to a delta table
telemetryDF.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", dbfsCheckpointDirPath)
  .partitionBy("device_id","telemetry_year", "telemetry_month", "telemetry_day")
  .start(dbfsDestDirPath)

// COMMAND ----------

// MAGIC %md
// MAGIC You can proceed to the next notebook in the set