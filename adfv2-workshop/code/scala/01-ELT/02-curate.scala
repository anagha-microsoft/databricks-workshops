// Databricks notebook source
// MAGIC %md
// MAGIC ## Curate NYC taxi data
// MAGIC In a typical data engineering pipeline, we do load followed by transformation (and more).  In this notebook - we will do some basic transformations and augmenting of the raw NYC taxi dataset.<br>
// MAGIC This is by no way complete, its merely for the purpose of demonstrating chaining a workflow.

// COMMAND ----------

// MAGIC %md ##### 1.  Drop records with missing timestamp

// COMMAND ----------

//REVIEW
//Filtering for records that have pickup and drop time as null
//val totalTripCount = spark.sql("select * from nyc_db.taxi_trips_raw").count //9710124
//val nullTimestampCount=spark.sql("select * from nyc_db.taxi_trips_raw").filter($"tpep_pickup_timestamp".isNull or $"tpep_dropoff_timestamp".isNull).count //9650 trips

// COMMAND ----------

//Filtering out records where pickup and dropoff date is null
val transformStep1DF=spark.sql("select * from nyc_db.taxi_trips_raw").filter($"tpep_pickup_timestamp".isNotNull or $"tpep_dropoff_timestamp".isNotNull)

// COMMAND ----------

//REVIEW
//transformStep1DF.count //9700474

// COMMAND ----------

// MAGIC %md ##### 2.  Drop records with passenger count of zero

// COMMAND ----------

val transformStep2DF=transformStep1DF.filter($"passenger_count"=!=0)

// COMMAND ----------

//REVIEW
//transformStep2DF.count //9690285

// COMMAND ----------

// MAGIC %md ##### 3.  Augment with temporal identifiers

// COMMAND ----------

// Using Spark SQL built in functions, augment with temporal attributes, based off of trip pick up timestamp
import org.apache.spark.sql.functions._
val transformStep3DF = transformStep2DF.withColumn("trip_hour", hour(col("tpep_pickup_timestamp")))
                                 .withColumn("time_bucket", 
                                             when($"trip_hour".geq(lit(0)) and $"trip_hour".lt(lit(4)),"00-TO-04")
                                             .when($"trip_hour".geq(lit(4)) and $"trip_hour".lt(lit(8)),"04-TO-08")
                                             .when($"trip_hour".geq(lit(8)) and $"trip_hour".lt(lit(12)),"08-TO-12")
                                             .when($"trip_hour".geq(lit(12)) and $"trip_hour".lt(lit(16)),"12-TO-16")
                                             .when($"trip_hour".geq(lit(16)) and $"trip_hour".lt(lit(20)),"16-TO-20")
                                             .when($"trip_hour".geq(lit(20)) and $"trip_hour".lt(lit(24)),"20-TO-24"))

// COMMAND ----------

//REVIEW
val timeBucketTrendDF=transformStep3DF.groupBy("time_bucket").count
display(timeBucketTrendDF)

// COMMAND ----------

// MAGIC %md ##### 4.  Persist the curated dataset

// COMMAND ----------

//Persist to the "curated" information zone as parquet
val dbfsDestDirPath = "/mnt/workshop/curated/nyctaxitrips"
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
transformStep3DF.coalesce(2).write.parquet(dbfsDestDirPath)

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
import com.databricks.backend.daemon.dbutils.FileInfo
dbutils.fs.ls(dbfsDestDirPath + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 5.  Define an external hive table on the data

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS nyc_db;
// MAGIC 
// MAGIC USE nyc_db;
// MAGIC DROP TABLE IF EXISTS taxi_trips_curated;
// MAGIC CREATE TABLE taxi_trips_curated
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/curated/nyctaxitrips");

// COMMAND ----------

//%sql
//select * from nyc_db.taxi_trips_curated

// COMMAND ----------

dbutils.notebook.exit("Pass")