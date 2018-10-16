// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 2 of a set of notebooks that demonstrate bulk load from blob storage into Azure Cosmos DB Cassandra API - of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC In this module, we will read the curated dataset and persist to Azure Cosmos DB Cassandra API.<br>
// MAGIC   
// MAGIC The following are tuning considerations:<br>
// MAGIC 1.  Right-size your Spark cluster to meet the SLA<br>
// MAGIC 2.  Right-size Azure Cosmos DB throughut provisioning<br>
// MAGIC 3.  Tune the Datastax Spark connnector write parameters detailed at the link below, in your Spark code-<BR>
// MAGIC  https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md

// COMMAND ----------

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.sql.cassandra._

//datastax Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

//CosmosDB library for multiple retry
import com.microsoft.azure.cosmosdb.cassandra

// Specify connection factory for Cassandra
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

// COMMAND ----------

// MAGIC %md
// MAGIC # 1.0. Persist to Azure Cosmos Db (Cassandra API)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### a) Quick refresher on source data

// COMMAND ----------

// MAGIC %sql
// MAGIC USE crimes_db;
// MAGIC refresh table CHICAGO_CRIMES_CURATED;
// MAGIC SELECT * FROM CHICAGO_CRIMES_CURATED;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### b) Create destination table if it does not already exist

// COMMAND ----------

val cdbConnector = CassandraConnector(sc)

//Create keyspace
cdbConnector.withSessionDo(session => session.execute("CREATE KEYSPACE IF NOT EXISTS crimes_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 } "))

// COMMAND ----------

//Create table
val tableDDL = "create table if not exists crimes_ks.crimes_chicago(case_id int primary key, case_nbr text, case_dt_tm text, block text, iucr text, primary_type text, description text, location_description text, arrest_made boolean, was_domestic boolean, beat int, district int, ward int, community_area int, fbi_code text, x_coordinate int, y_coordinate int, case_year int, updated_dt text, latitude double, longitude double, location_coords text, case_timestamp timestamp, case_month int, case_day_of_month int, case_hour int, case_day_of_week_nbr int, case_day_of_week_name text), with cosmosdb_provisioned_throughput=10000)"

val cdbConnector = CassandraConnector(sc)
cdbConnector.withSessionDo(session => session.execute(tableDDL))

// COMMAND ----------

// MAGIC %md #####c) Bulk load

// COMMAND ----------

/* Run if you are doig bulk load of entire dataset
val cdbConnector = CassandraConnector(sc)
cdbConnector.withSessionDo(session => session.execute("ALTER TABLE crimes_ks.crimes_chicago WITH cosmosdb_provisioned_throughput=50000"))
Thread.sleep(10000)
*/

// COMMAND ----------

import java.util.Calendar

// Write tuning parameters
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")//Leave this as one for Cosmos DB
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "2")//Maximum number of connections per Host set on each Executor JVM - default parallelism/executors for Spark Commands
spark.conf.set("spark.cassandra.output.concurrent.writes", "5")//Maximum number of batches executed in parallel by a single Spark task
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "300")//How many batches per single Spark task can be stored in memory before sending to Cassandra
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "5000") //Period of time to keep unused connections open

//Full dataset
//val sourceDF = spark.sql("SELECT * FROM crimes_db.chicago_crimes_curated")

//Pruning the complete dataset to smaller
val sourceDF = spark.sql("SELECT * FROM crimes_db.chicago_crimes_curated where district = '007'")
sourceDF.write
  .mode("append")
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "crimes_chicago", "keyspace" -> "crimes_ks"))
  .save()

println("End time=" + Calendar.getInstance().getTime())

// COMMAND ----------

// MAGIC %md ##### d) Run a count on the table

// COMMAND ----------

//Not supported
/*
spark.conf.set("spark.cassandra.concurrent.reads", "512")//Sets read parallelism 
spark.conf.set("spark.cassandra.splitCount", "250")//Specify the number of Spark partitions to read the Cassandra table into
spark.conf.set("spark.cassandra.input.split.size_in_mb", "64")//Approx amount of data to be fetched into a Spark partition
spark.conf.set("spark.cassandra.input.reads_per_sec", "100000")//Sets max requests per core per second
spark.conf.set("spark.cassandra.input.fetch.size_in_rows", "250")//Number of CQL rows fetched per driver request

sc.cassandraTable("crimes_ks", "crimes_chicago").count
*/