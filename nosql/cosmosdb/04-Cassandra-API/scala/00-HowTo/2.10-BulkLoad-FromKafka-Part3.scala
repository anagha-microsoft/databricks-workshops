// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 3 of 3 notebooks that demonstrate bulk load from Kafka, in batch mode, of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In notebook 1, we published data to Kafka for purpose of the exercise<BR>
// MAGIC - In notebook 2, we read from Kafka and persisted to Azure Cosmos DB Cassandra API<BR>
// MAGIC - In this notebook, we will read from Kafka and write to a Databricks Delta table<BR>
// MAGIC   
// MAGIC While the Azure Cosmos DB Cassandra table serves as a hot store for OLTP, the Delta table will serve as an analytics store.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read from Kafka in batch

// COMMAND ----------

val kafkaTopic = "crimes_chicago_topic"
val kafkaBrokerAndPortCSV = "10.1.0.11:9092, 10.1.0.12:9092,10.1.0.13:9092,10.1.0.14:9092"

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val kafkaSourceDF = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokerAndPortCSV)
  .option("subscribe", kafkaTopic)
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
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

consumableDF.show

// COMMAND ----------

consumableDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Sink to Databricks Delta

// COMMAND ----------

//Took the author 3 minutes

//1) Destination directory for delta table
val dbfsDestDirPath="/mnt/data/crimes/curatedDir/chicago-crimes-data-delta"

//2) Remove output from prior execution
dbutils.fs.rm(dbfsDestDirPath, recurse=true)

//3) Persist as delta format (Parquet) to curated zone to a delta table
consumableDF.write.format("delta").mode("overwrite").save(dbfsDestDirPath)

// COMMAND ----------

// MAGIC %sql
// MAGIC --Took the author 2 minutes for 1.5 GB of data/6.7 M rows
// MAGIC CREATE DATABASE IF NOT EXISTS crimes_db;
// MAGIC 
// MAGIC USE crimes_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS chicago_crimes_delta;
// MAGIC CREATE TABLE chicago_crimes_delta
// MAGIC USING DELTA
// MAGIC LOCATION 'mnt/data/crimes/curatedDir/chicago-crimes-data-delta';
// MAGIC 
// MAGIC OPTIMIZE chicago_crimes_delta;
// MAGIC 
// MAGIC select * from crimes_db.chicago_crimes_delta;