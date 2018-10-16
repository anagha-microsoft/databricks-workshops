// Databricks notebook source
// MAGIC %md
// MAGIC This is part 1 of 3 notebooks that demonstrate stream ingest from Kafka, of live telemetry from Azure IoT hub.<BR>
// MAGIC - In **this notebook**, we will ingest from Kafka into Azure Cosmos DB - SQL API (OLTP document-oriented nosql store), the latest telemetry received<BR>
// MAGIC - In notebook 2, we will ingest from Kafka using structured stream processing and persist every event to a Databricks Delta table (analytics store)<BR>
// MAGIC - In notebook 3, we will run queries against the Delta table<BR>
// MAGIC   
// MAGIC Reference: [Taking Structured Streaming to Production](https://databricks.com/blog/2017/05/18/taking-apache-sparks-structured-structured-streaming-to-production.html) | [Cosmos DB configuration for Spark](https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read from Kafka with spark structured streaming

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val kafkaTopic = "iot_telemetry_in"
//Replace with your Kafka cluster broker IPs
val kafkaBrokerAndPortCSV = "10.1.0.11:9092, 10.1.0.12:9092,10.1.0.13:9092,10.1.0.14:9092"

val kafkaSourceDF = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokerAndPortCSV)
  .option("subscribe", kafkaTopic)
  .option("startingOffsets", "latest")
  .load()

val rawDF = kafkaSourceDF.selectExpr("CAST(value AS STRING) as iot_payload").as[(String)]

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Parse events from kafka

// COMMAND ----------

def parseDeviceTelemetry(payloadString: String): (String,String,String,String,String,String,String,String,String) = {
  val device_id=payloadString.substring(payloadString.indexOf("=")+1,payloadString.indexOf(","))
  val telemetry_ts=payloadString.substring(payloadString.indexOf("enqueuedTime=")+13,payloadString.indexOf(",sequenceNumber="))
  val telemetry_json=payloadString.substring(payloadString.indexOf(",content=")+9,payloadString.indexOf(",systemProperties="))
  //Return tuple
  (
     device_id,
     telemetry_ts,
     telemetry_json,//json with temperature, humidity and pressure reading
     telemetry_ts.substring(0,4),//telemetry_year
     telemetry_ts.substring(5,7),//telemetry_month
     telemetry_ts.substring(8,10),//telemetry_day
     telemetry_ts.substring(11,13),//telemetry_hour
     telemetry_ts.substring(14,16)//telemetry_minute
     ,device_id //id
  )
}

// COMMAND ----------

val parsedDF=rawDF.map(x => parseDeviceTelemetry(x)).toDF("device_id","telemetry_ts","telemetry_json","telemetry_year","telemetry_month","telemetry_day","telemetry_hour","telemetry_minute","id")
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
                                  $"telemetry_minute",
                                  $"id")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Persist the stream to Azure Cosmos DB - SQL API (Document store)

// COMMAND ----------

import org.apache.spark.sql.functions._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.codehaus.jackson.map.ObjectMapper
import com.microsoft.azure.cosmosdb.spark.streaming._
import org.apache.spark.sql.streaming.Trigger

//Cosmos DB conf
val cosmosDbWriteConfigMap = Map(
  "Endpoint" -> spark.conf.get("cdbEndpoint"),
  "Masterkey" -> spark.conf.get("cdbAccessKey"),
  "Database" -> "telemetry_db",
  "Collection" -> "telemetry_curr_state_store",
  "Upsert" -> "true")
//,"WritingBatchSize" -> 1000)//Not working currently - contact Cosmos DB ninja team for support


//Checkpoint directory
val dbfsCheckpointDirPath="/mnt/data/iot/checkpointDir/telemetry-cosmosdb/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

val query = telemetryDF
  .writeStream
  .queryName("IoT-Telemetry-Persistence")
  .outputMode("append")
  .format(classOf[CosmosDBSinkProvider].getName).options(cosmosDbWriteConfigMap)
  .option("checkpointLocation",dbfsCheckpointDirPath)
  .trigger(Trigger.ProcessingTime(1000 * 5)) 
  .start()