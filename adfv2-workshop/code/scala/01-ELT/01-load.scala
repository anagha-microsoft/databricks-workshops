// Databricks notebook source
// MAGIC %md
// MAGIC ## Load NYC taxi data
// MAGIC This notebook reads an external source storage account (we will refer to as "staging") with NYC taxi data in CSV format, infer schema and persist to the "raw" information zone, as parquet.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.  Define (external) source/staging storage location

// COMMAND ----------

//Staging zone configuration
val storageAccountName = "taxinydata"
val containerName = "taxidata"
var fileName = "NYC_Cab.csv"
val storageAccountAccessKey = dbutils.secrets.get(scope = "source-blob-storage", key = "storage-acct-key") 
spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.  Read data in staging location

// COMMAND ----------

//import java.io.FileReader
//import java.io.FileNotFoundException

import java.io.IOException

var rawTaxiTripsDF : org.apache.spark.sql.DataFrame = null
try {
   //importing data from blob
   val blobFilePath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/${fileName}"
   rawTaxiTripsDF= spark.read.option("header","true").csv(blobFilePath)
  
}catch {
  case ex: IOException => {
            println("Invalid Blob Connection")} 
}

// COMMAND ----------

//Explore
//rawTaxiTripsDF.count
//display(rawTaxiTripsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 3.  Cast to appropriate format

// COMMAND ----------

//Cast the columns to the right datatype, rename fields as needed
import org.apache.spark.sql.types._
val typecastedRawTaxiTripsDF=rawTaxiTripsDF.select($"tripID",
                               $"VendorID",
                               $"tpep_pickup_datetime".cast(TimestampType),
                               $"tpep_dropoff_datetime".cast(TimestampType),
                               $"passenger_count".cast(IntegerType),
                               $"trip_distance".cast(DoubleType),
                               $"RatecodeID",
                               $"store_and_fwd_flag",
                               $"PULocationID",
                               $"DOLocationID",
                               $"payment_type".cast(IntegerType))toDF("trip_id", "vendor_id", "tpep_pickup_timestamp", "tpep_dropoff_timestamp", "passenger_count", "trip_distance", "rate_code_id", "store_and_fwd_flag", "pu_location_id", "dropoff_location_id", "payment_type")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.  Persist as parquet to the "raw" information zone

// COMMAND ----------

//Persist to the "raw" information zone as parquet
val dbfsDestDirPath = "/mnt/workshop/raw/nyctaxitrips"
dbutils.fs.rm(dbfsDestDirPath, recurse=true)
typecastedRawTaxiTripsDF.coalesce(2).write.parquet(dbfsDestDirPath)

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
// MAGIC DROP TABLE IF EXISTS taxi_trips_raw;
// MAGIC CREATE TABLE taxi_trips_raw
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/raw/nyctaxitrips");

// COMMAND ----------

// MAGIC %sql
// MAGIC --select * from nyc_db.taxi_trips_raw

// COMMAND ----------

dbutils.notebook.exit("Pass")