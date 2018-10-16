// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 2 of 3 notebooks that demonstrate bulk load from Kafka, in batch mode, of 6.7 million records/1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In notebook 1, we published data to Kafka for purpose of the exercise<BR>
// MAGIC - In this notebook, we will read from Kafka and persist to Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 3, we will read from Kafka and persist to a Databricks Delta table<BR>
// MAGIC   
// MAGIC With the three notebooks, we cover (publishing to Kafka and then), reading from Kafka in batch mode and sinking to an OLTP store - the Azure Cosmos DB Cassandra table, and an analytics store - the Databricks Delta table.<BR>

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
// MAGIC ### 3.0. Persist to Azure Cosmos DB Cassandra API

// COMMAND ----------

//Author's attempt: 
//2 hours
//Provisioning specs - Databricks: Runtime 4.3, 8 DS3v2 workers with option to scale to 10 workers, 
//Provisioning specs - Cosmos DB: Started the table with 10K RUs;  Altered table to 10,000 RUs per physical partition created
//Connector conf - spark.cassandra.output.batch.size.rows=1; spark.cassandra.connection.connections_per_executor_max=2; spark.cassandra.output.concurrent.writes=2;spark.cassandra.output.batch.grouping.buffer.size=300
//The above needs tuning for sure


//datastax Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

//CosmosDB library for multiple retry
import com.microsoft.azure.cosmosdb.cassandra

//Specify connection factory for Cassandra
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

import java.util.Calendar

//Write tuning parameters
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")//Leave this as one for Cosmos DB
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "2")//Maximum number of connections per Host set on each Executor JVM - default parallelism/executors for Spark Commands
spark.conf.set("spark.cassandra.output.concurrent.writes", "2")//Maximum number of batches executed in parallel by a single Spark task
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
