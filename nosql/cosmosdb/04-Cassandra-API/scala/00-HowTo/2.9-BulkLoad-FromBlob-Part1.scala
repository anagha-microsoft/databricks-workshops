// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 1 or 2 notebooks that demonstrate bulk load from blob storage into Azure Cosmos DB Cassandra API - of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC The followng are the steps covered in this module, essentially data engineering work:<br>
// MAGIC 
// MAGIC 1. Download the dataset to the driver node
// MAGIC 2. Upload to DBFS to the raw data persistence zone as parquet, create a Hive table on the raw dataset, optimize it
// MAGIC 3. Format the raw dataset, persist to the curated data zone as parquet, create a Hive table on the curated dataset
// MAGIC <br>We will use this dataset to load to Azure Cosmos DB Cassandra API.
// MAGIC 
// MAGIC **Chicago crimes dataset:**<br>
// MAGIC Website: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2<br>
// MAGIC Dataset: https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD<br>
// MAGIC Metadata: https://cosmosdbworkshops.blob.core.windows.net/metadata/ChicagoCrimesMetadata.pdf<br>

// COMMAND ----------

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Download public dataset to the driver node

// COMMAND ----------

//1) Local directory to download to
val localDirPath="/tmp/downloads/chicago-crimes-data/"
dbutils.fs.rm(localDirPath, recurse=true)
dbutils.fs.mkdirs(localDirPath)

// COMMAND ----------

//2) Download to driver local file system
//Download of 1.58 GB of data - took the author ~10 minutes
import sys.process._
import scala.sys.process.ProcessLogger

val wgetToExec = "wget -P " + localDirPath + " https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"
println(wgetToExec)
wgetToExec !!

// COMMAND ----------

// MAGIC %fs
// MAGIC ls file:/tmp/downloads/chicago-crimes-data/

// COMMAND ----------

// MAGIC %fs
// MAGIC mv file:/tmp/downloads/chicago-crimes-data/rows.csv?accessType=DOWNLOAD file:/tmp/downloads/chicago-crimes-data/chicago-crimes.csv

// COMMAND ----------

//3) Filename
val localFile="file:/tmp/downloads/chicago-crimes-data/chicago-crimes.csv"

//4) List the download
display(dbutils.fs.ls(localFile))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.  Upload from driver node to DBFS

// COMMAND ----------

//1) Create destination directory
val dbfsDirPath="/mnt/data/crimes/stagingDir/chicago-crimes-data"
dbutils.fs.rm(dbfsDirPath, recurse=true)
dbutils.fs.mkdirs(dbfsDirPath)

// COMMAND ----------

//2) Upload to from localDirPath to dbfsDirPath
dbutils.fs.cp(localFile, dbfsDirPath, recurse=true)

//3) Clean up local directory
//dbutils.fs.rm(localFile)

//4) List dbfsDirPath
display(dbutils.fs.ls(dbfsDirPath))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Read raw CSV, persist to parquet

// COMMAND ----------

//1) Source directory
val dbfsSrcDirPath="/mnt/data/crimes/stagingDir/chicago-crimes-data"

//2) Destination directory
val dbfsDestDirPath="/mnt/data/crimes/rawDir/chicago-crimes-data"

// COMMAND ----------

dbutils.fs.head(dbfsSrcDirPath + "/chicago-crimes.csv")

// COMMAND ----------

//3) Define source schema
// Metadata is at - https://cosmosdbworkshops.blob.core.windows.net/metadata/ChicagoCrimesMetadata.pdf
// Always a good idea/practice to provide schema
// We will use inferSchema in our example

/*
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType,BooleanType}

val crimesSchema = StructType(Array(
    StructField("CASE_ID", IntegerType, true),
    StructField("CASE_NBR", StringType, true),
    StructField("CASE_DT_TM", StringType, true),
    StructField("BLOCK", StringType, true),
  StructField("IUCR", StringType, true),
    StructField("PRIMARY_TYPE", StringType, true),
    StructField("CRIME_DESCRIPTION", StringType, true),
    StructField("LOCATION_DESCRIPTION", StringType, true),
  StructField("ARREST_MADE", BooleanType, true),
    StructField("WAS_DOMESTIC", BooleanType, true),
    StructField("BEAT", IntegerType, true),
    StructField("DISTRICT", IntegerType, true),
  StructField("WARD", IntegerType, true),
    StructField("COMMUNITY_AREA", IntegerType, true),
    StructField("FBI_CODE", StringType, true),
    StructField("X_COORDINATE", IntegerType, true),
    StructField("Y_COORDINATE", IntegerType, true),
    StructField("YEAR", IntegerType, true),
    StructField("UPDATED_DT", StringType, true),
    StructField("LATITUDE", DoubleType, true),
    StructField("LONGITUDE", DoubleType, true),
    StructField("LOCATION_COORDINATES", StringType, true)))
    */

// COMMAND ----------

//4)  Read raw CSV
val sourceDF = spark.read.format("csv")
  .option("header", "true").option("header", "true").load(dbfsSrcDirPath).toDF("case_id", "case_nbr", "case_dt_tm", "block", "iucr", "primary_type", "description", "location_description", "arrest_made", "was_domestic", "beat", "district", "ward", "community_area", "fbi_code", "x_coordinate", "y_coordinate", "case_year", "updated_dt", "latitude", "longitude", "location_coords")

//.schema(crimesSchema)- in case you want to explicitly provide your own schema

sourceDF.printSchema
sourceDF.show

// COMMAND ----------

//5) Persist as parquet to raw zone
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
sourceDF.coalesce(1).write.parquet(dbfsDestDirPath)

//6) Delete residual files from job operation (_SUCCESS, _start*, _committed*)
import com.databricks.backend.daemon.dbutils.FileInfo
dbutils.fs.ls(dbfsDestDirPath + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

display(dbutils.fs.ls(dbfsDestDirPath))

// COMMAND ----------

//7) Create external table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS CRIMES_DB;
// MAGIC 
// MAGIC USE CRIMES_DB;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS CHICAGO_CRIMES_RAW;
// MAGIC CREATE TABLE IF NOT EXISTS CHICAGO_CRIMES_RAW
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/data/crimes/rawDir/chicago-crimes-data");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC ANALYZE TABLE CHICAGO_CRIMES_RAW COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %sql
// MAGIC USE crimes_db;
// MAGIC SELECT * FROM chicago_crimes_raw;
// MAGIC --SELECT count(*) FROM chicago_crimes_raw;--6,701,049

// COMMAND ----------

// MAGIC  %md
// MAGIC  ### 1.0.2. Curate
// MAGIC  In this section, we will just parse the date and time for the purpose of analytics.

// COMMAND ----------

// 1) Read and curate

import org.apache.spark.sql.functions.to_timestamp

val to_timestamp_func = to_timestamp($"case_dt_tm", "MM/dd/yyyy hh:mm:ss")
val rawDF = spark.sql("select * from crimes_db.chicago_crimes_raw")
val curatedDF = rawDF.withColumn("case_timestamp",to_timestamp_func)
                      .withColumn("case_month", month(col("case_timestamp")))
                      .withColumn("case_day_of_month", dayofmonth(col("case_timestamp")))
                      .withColumn("case_hour", hour(col("case_timestamp")))
                      .withColumn("case_day_of_week_nbr", dayofweek(col("case_timestamp")))
                      .withColumn("case_day_of_week_name", when(col("case_day_of_week_nbr") === lit(1), "Sunday")
                                                          .when(col("case_day_of_week_nbr") === lit(2), "Monday")
                                                          .when(col("case_day_of_week_nbr") === lit(3), "Tuesday")
                                                          .when(col("case_day_of_week_nbr") === lit(4), "Wednesday")
                                                          .when(col("case_day_of_week_nbr") === lit(5), "Thursday")
                                                          .when(col("case_day_of_week_nbr") === lit(6), "Friday")
                                                          .when(col("case_day_of_week_nbr") === lit(7), "Sunday")
                                 )
                                                          
curatedDF.printSchema
curatedDF.show  

// COMMAND ----------

//2) Persist as parquet to raw zone
val dbfsDestDirPath="/mnt/data/crimes/curatedDir/chicago-crimes-data"
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
curatedDF.coalesce(1).write.parquet(dbfsDestDirPath)

//3) Delete residual files from job operation (_SUCCESS, _start*, _committed*)
import com.databricks.backend.daemon.dbutils.FileInfo
dbutils.fs.ls(dbfsDestDirPath + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

//4) List
display(dbutils.fs.ls(dbfsDestDirPath))

// COMMAND ----------

//5) Create external table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS CRIMES_DB;
// MAGIC 
// MAGIC USE CRIMES_DB;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS CHICAGO_CRIMES_CURATED;
// MAGIC CREATE TABLE CHICAGO_CRIMES_CURATED
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/data/crimes/curatedDir/chicago-crimes-data");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC REFRESH TABLE CHICAGO_CRIMES_CURATED;
// MAGIC ANALYZE TABLE CHICAGO_CRIMES_CURATED COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %sql
// MAGIC USE crimes_db;
// MAGIC SELECT * FROM CHICAGO_CRIMES_CURATED;
// MAGIC --SELECT count(*) FROM CHICAGO_CRIMES_CURATED;--6,701,049

// COMMAND ----------

// MAGIC %sql
// MAGIC USE crimes_db;
// MAGIC DESCRIBE CHICAGO_CRIMES_CURATED;

// COMMAND ----------

// MAGIC %md
// MAGIC We are now good to go, to bulk insert into Azure Cosmos DB Cassandra API. <BR>
// MAGIC Proceed to part 2 of the lab for bulk load.