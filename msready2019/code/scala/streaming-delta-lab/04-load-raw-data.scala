// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this notebook?
// MAGIC We will: 
// MAGIC 1.  Read reference data in staging directory, parse, and persist to "raw" information zone as parquet<br>
// MAGIC 2.  We will create a Hive external table on top of this data<br>
// MAGIC 3.  And validate queryability, for subsequent use in the workshop<br>

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}

//1.  Define schema for taxi zone lookup
val taxiZoneSchema = StructType(Array(
    StructField("location_id", StringType, true),
    StructField("borough", StringType, true),
    StructField("zone", StringType, true),
    StructField("service_zone", StringType, true)))

// COMMAND ----------

//2.  Read source data
  val refDF = spark.read.option("header", "true")
                      .schema(taxiZoneSchema)
                      .option("delimiter",",")
                      .csv("/mnt/workshop/staging/reference-data/")

// COMMAND ----------

// 3. Review
display(refDF)

// COMMAND ----------

//4.  Persist as parquet

//Define destination and clean up
val  dbfsDestinationDirectory = "/mnt/workshop/curated/reference-data/taxi-zone/"
dbutils.fs.rm(dbfsDestinationDirectory, recurse=true)

//Persist
refDF.coalesce(1).write.parquet(dbfsDestinationDirectory)

// COMMAND ----------

//5. Create Hive external table

// COMMAND ----------

// MAGIC %sql 
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC 
// MAGIC USE taxi_db;
// MAGIC DROP TABLE IF EXISTS taxi_zone_lookup;
// MAGIC CREATE TABLE IF NOT EXISTS taxi_zone_lookup(
// MAGIC location_id STRING,
// MAGIC borough STRING,
// MAGIC zone STRING,
// MAGIC service_zone STRING)
// MAGIC USING parquet
// MAGIC LOCATION '/mnt/workshop/curated/reference-data/taxi-zone/';
// MAGIC 
// MAGIC ANALYZE TABLE taxi_zone_lookup COMPUTE STATISTICS;

// COMMAND ----------

//6.  Explore

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_db.taxi_zone_lookup

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from taxi_db.taxi_zone_lookup