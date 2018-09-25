// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 2 of 3 notebooks that demonstrate bulk load from Kafka, in batch mode, of 6.7 million records/1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In notebook 1, we published data to Kafka for purpose of the exercise<BR>
// MAGIC - In this notebook, we will read from kafka and persist to Azure Cosmos DB Cassandra API<BR>
// MAGIC - In notebook 3, we will read from Kafka and write to a Databricks Delta table<BR>
// MAGIC   
// MAGIC   
// MAGIC With the three notebooks, we cover (publishing to Kafka) sinking to an OLTP store - the Azure Cosmos DB Cassandra table, and an analytics store - the Databricks Delta table.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read from Kafka in batch

// COMMAND ----------

val kafkaTopic = "crimes_chicago_topic"
val kafkaBrokerAndPortCSV = "10.7.0.4:9092, 10.7.0.5:9092,10.7.0.8:9092,10.7.0.12:9092"

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
// MAGIC ### 3.0. Sample to write to Databricks Delta

// COMMAND ----------

/*  TO WRITE TO DATARICKS DELTA
//Took the author <3 minutes

//1) Destination directory for delta table
val dbfsDestDirPath="/mnt/data/crimes/curatedDir/chicago-crimes-data-delta"

//2) Remove output from prior execution
dbutils.fs.rm(dbfsDestDirPath, recurse=true)

//3) Persist as delta format (Parquet) to curated zone to a delta table
consumableDF.write.format("delta").save(dbfsDestDirPath)

%sql
--Took the author 2.68 minutes for 1.5 GB of data/6.7 M rows
CREATE DATABASE IF NOT EXISTS crimes_db;

USE crimes_db;

DROP TABLE IF EXISTS chicago_crimes_delta;
CREATE TABLE chicago_crimes_delta
USING DELTA
LOCATION 'mnt/data/crimes/curatedDir/chicago-crimes-data-delta';

OPTIMIZE chicago_crimes_delta;

select * from crimes_db.chicago_crimes_delta;
*/

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Persist to Azure Cosmos DB Cassandra API

// COMMAND ----------

 //datastax Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

//CosmosDB library for multiple retry
import com.microsoft.azure.cosmosdb.cassandra

// Specify connection factory for Cassandra
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

import java.util.Calendar

// Write tuning parameters
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")//Leave this as one for Cosmos DB
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "2")//Maximum number of connections per Host set on each Executor JVM - default parallelism/executors for Spark Commands
spark.conf.set("spark.cassandra.output.concurrent.writes", "5")//Maximum number of batches executed in parallel by a single Spark task
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "300")//How many batches per single Spark task can be stored in memory before sending to Cassandra
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "5000") //Period of time to keep unused connections open
//cosmosdb_provisioned_throughput=500000

println("Start time=" + Calendar.getInstance().getTime())

consumableDF.write
  .mode("append")
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "crimes_chicago_batch", "keyspace" -> "crimes_ks"))
  .save()

println("End time=" + Calendar.getInstance().getTime())
