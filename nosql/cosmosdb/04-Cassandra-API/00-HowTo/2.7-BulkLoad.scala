// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # WORK IN PROGRESS

// COMMAND ----------

// MAGIC %run ../00-HowTo/2-CassandraUtils

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

// Parallelism and throughput configs
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "10")
spark.conf.set("spark.cassandra.output.concurrent.writes", "1000")
spark.conf.set("spark.cassandra.concurrent.reads", "512")
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "1000")
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "600000000") //Increase this number as needed
spark.conf.set("spark.cassandra.output.ignoreNulls","true")
spark.conf.set("spark.cassandra.output.consistency.level","ALL")//Write consistency = Strong
spark.conf.set("spark.cassandra.input.consistency.level","ALL")//Read consistency = Strong

// COMMAND ----------

// MAGIC %md
// MAGIC #1.0. Bulk load from blob storage<BR>
// MAGIC In this exercise, we will download 1.5 GB of Chicago crimes dataset and load them to CosmosDB Cassandra API<br>
// MAGIC Website: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2<br>
// MAGIC Dataset: https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD<br>
// MAGIC Metadata: https://cosmosdbworkshops.blob.core.windows.net/metadata/ChicagoCrimesMetadata.pdf<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.1. Create directory to download data to

// COMMAND ----------

//DBFS
val dbfsDirPath="/mnt/data/movielens/scratchDir/chicago-crimes-data"
dbutils.fs.rm(dbfsDirPath, recurse=true)
dbutils.fs.mkdirs(dbfsDirPath)

// COMMAND ----------

// MAGIC %fs ls "/mnt/data/movielens/scratchDir/"

// COMMAND ----------

//Local
val localDirPath="/tmp/downloads/chicago-crimes-data"
dbutils.fs.rm(localDirPath, recurse=true)
dbutils.fs.mkdirs(localDirPath)
//dbutils.fs.ls(localDirPath)

// COMMAND ----------

// MAGIC %fs ls "/tmp/downloads"

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.2. Download data to local direactory from the internet

// COMMAND ----------

//Download 1.58 GB of data - took the author ~14 minutes
import sys.process._
import scala.sys.process.ProcessLogger

"wget -P /tmp/downloads/chicago-crimes-data https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD" !!

// COMMAND ----------

//ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location
//10000092,HY189866,03/18/2015 07:44:00 PM,047XX W OHIO ST,041A,BATTERY,AGGRAVATED: HANDGUN,STREET,false,false,1111,011,28,25,04B,1144606,1903566,2015,02/10/2018 03:50:01 PM,41.891398861,-87.744384567,"(41.891398861, -87.744384567)"


// COMMAND ----------

// MAGIC %md
// MAGIC #2.0. Bulk load from RDBMS

// COMMAND ----------

