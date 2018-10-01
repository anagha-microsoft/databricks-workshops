// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 4 of 4 notebooks that demonstrate stream ingest from Kafka, of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In notebook 1, we published data to Kafka for purpose of the exercise<BR>
// MAGIC - In notebook 2, we attempted to ingest from Kafka using structured stream processing and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 3, we ingest from Kafka using classic stream processing (DStream based) and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In **this notebook**, we will ingest from Kafka using structured stream processing and persist to a Databricks Delta table<BR>
// MAGIC   
// MAGIC While the Azure Cosmos DB Cassandra table serves as a hot store for OLTP, the Delta table will serve as an analytics store.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read stream from Kafka

// COMMAND ----------

val kafkaTopic = "crimes_chicago_topic"
val kafkaBrokerAndPortCSV = "10.7.0.4:9092, 10.7.0.5:9092,10.7.0.8:9092,10.7.0.12:9092"

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

val formatedKafkaDF = kafkaSourceDF.selectExpr("CAST(key AS STRING) as case_id", "CAST(value AS STRING) as json_payload")
  .as[(String, String)]

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Parse events from kafka

// COMMAND ----------

val consumableDF = formatedKafkaDF.select(get_json_object($"json_payload", "$.case_id").cast(IntegerType).alias("case_id"),
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
                                          get_json_object($"json_payload", "$.case_day_of_week_name").alias("case_day_of_week_name")
                                          )

// COMMAND ----------

consumableDF.printSchema

// COMMAND ----------

//consumableDF.show
//org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Sink to Databricks Delta

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 3.0.1. Sink to delta table

// COMMAND ----------

//1) Destination directory for delta table, and for checkpoints
val dbfsDestDirPath="/mnt/data/crimes/curatedDir/chicago-crimes-stream-delta/"
val dbfsCheckpointDirPath="/mnt/data/crimes/checkpointDir/chicago-crimes-stream-delta/"

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
// MAGIC Run the below in a different notebook while the stream process is running:

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS crimes_db;
// MAGIC 
// MAGIC USE crimes_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS chicago_crimes_delta_stream;
// MAGIC CREATE TABLE chicago_crimes_delta_stream
// MAGIC USING DELTA
// MAGIC LOCATION 'mnt/data/crimes/curatedDir/chicago-crimes-stream-delta';
// MAGIC 
// MAGIC OPTIMIZE chicago_crimes_delta_stream;
// MAGIC 
// MAGIC select * from crimes_db.chicago_crimes_delta_stream;