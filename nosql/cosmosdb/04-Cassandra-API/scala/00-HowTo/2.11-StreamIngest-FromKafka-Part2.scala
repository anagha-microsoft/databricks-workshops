// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 2 of 4 notebooks that demonstrate stream ingest from Kafka, of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In notebook 1, we published data to Kafka for purpose of the exercise<BR>
// MAGIC - In **this notebook**, we will (attempt to) ingest from Kafka using structured stream processing and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 3, we will ingest from Kafka using classic stream processing (DStream based) and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 4, we will ingest from Kafka using structured stream processing and persist to a Databricks Delta table<BR>
// MAGIC   
// MAGIC While the Azure Cosmos DB Cassandra table serves as a hot store for OLTP, the Delta table will serve as an analytics store.
// MAGIC   
// MAGIC ### Note:
// MAGIC At the time of authoring this notebook (Oct 2018), **neither documented path to structured stream sink to Cassandra worked** (Spark 2.3.1, connector 2.11-2.3.2).<br>
// MAGIC https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/spark/structuredStreaming.html<br>
// MAGIC The published samples work only with connector and spark version 2.3.0; Unfortunately, the Datastax connector for Spark 2.3.0 does not work with Databricks runtime with Spark 2.3.0.<br>
// MAGIC The connector author is working on forward compatibility tracked under [JIRA](https://www.google.com/url?q=https%3A%2F%2Fdatastax.jira.com%2Fbrowse%2FDSP-16635&sa=D&sntz=1&usg=AFQjCNED-nPzpF0jwURzMq7ibtNFQKfAXA).<br>
// MAGIC Reference: [Structured streaming sink related thread](https://groups.google.com/a/lists.datastax.com/forum/#!msg/spark-connector-user/0aHJ4oskw7Q/xPKoqrtVAgAJ)<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read from Kafka with spark structured streaming

// COMMAND ----------

val kafkaTopic = "crimes_chicago_topic"
//Replace the list below with yours
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

// MAGIC %md
// MAGIC ### 3.0. Persist the stream to Azure Cosmos DB Cassandra API

// COMMAND ----------

//1) Destination directory for checkpoints
val dbfsCheckpointDirPath="/mnt/data/crimes/checkpointDir/chicago-crimes-stream-cassandra/"

//2) Remove output from prior execution
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

// COMMAND ----------

//Author's attempt: 
//Provisioning specs - Databricks: Runtime 4.3, 8 DS3v2 workers with option to scale to 10 workers, 
//Provisioning specs - Cosmos DB: Started the table with 10K RUs;  Altered table to 10,000 RUs per physical partition created
//Connector conf - spark.cassandra.output.batch.size.rows=1; spark.cassandra.connection.connections_per_executor_max=2; spark.cassandra.output.concurrent.writes=2;spark.cassandra.output.batch.grouping.buffer.size=300
//The above needs tuning for sure


//datastax Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._

//CosmosDB library for multiple retry
import com.microsoft.azure.cosmosdb.cassandra

//Specify connection factory for Cassandra
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.Calendar

//Write tuning parameters
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")//Leave this as one for Cosmos DB
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "2")//Maximum number of connections per Host set on each Executor JVM - default parallelism/executors for Spark Commands
spark.conf.set("spark.cassandra.output.concurrent.writes", "2")//Maximum number of batches executed in parallel by a single Spark task
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "300")//How many batches per single Spark task can be stored in memory before sending to Cassandra
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "5000") //Period of time to keep unused connections open
//cosmosdb_provisioned_throughput=500000

/*
//Option 1
val query = consumableDF.writeStream
  .option("checkpointLocation", dbfsCheckpointDirPath)
 .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "crimes_chicago_stream", "keyspace" -> "crimes_ks"))
  .outputMode(OutputMode.Update)
  .start()
  
  //DOES NOT WORK; This probably only works with DSE;
  //java.lang.UnsupportedOperationException: Data source org.apache.spark.sql.cassandra does not support streamed writing
*/


val query = consumableDF.writeStream
  .cassandraFormat("crimes_chicago_stream", "crimes_ks")
  .option("checkpointLocation", dbfsCheckpointDirPath)
  .outputMode(OutputMode.Update)
  .start()
  