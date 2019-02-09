// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this notebook?
// MAGIC We will: 
// MAGIC 1.  Read reference data (Taxi zone lookup data) in staging directory, parse, and persist to "raw" information zone as parquet, create a Hive external table on top of this data, and validate queryability, for subsequent use in the workshop<br>
// MAGIC 1.  Read transactional data (NYC taxi trips) in staging directory, parse, and persist to "raw" information zone as parquet, create a Hive external table on top of this data, and validate queryability, for subsequent use in the workshop<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.0. Load reference data

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

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.0. Load transactional data (taxi trips)

// COMMAND ----------

// 1. Review dataset with "head" command
dbutils.fs.head("/mnt/workshop/staging/transactions/yellow_tripdata_2017-01.csv")

// COMMAND ----------

// 2.  Define schema
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType,BooleanType, DecimalType}

val tripSchema = StructType(Array(
    StructField("VendorID", StringType, true),
    StructField("tpep_pickup_datetime", TimestampType, true),
    StructField("tpep_dropoff_datetime", TimestampType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", FloatType, true),
    StructField("RatecodeID", StringType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("PULocationID", StringType, true),
    StructField("DOLocationID", StringType, true),
    StructField("payment_type", StringType, true),
    StructField("fare_amount", FloatType, true),
    StructField("extra", FloatType, true),
    StructField("mta_tax", FloatType, true),
    StructField("tip_amount", FloatType, true),
    StructField("tolls_amount", FloatType, true),
    StructField("improvement_surcharge", FloatType, true),
    StructField("total_amount", DoubleType, true)
))

// COMMAND ----------

// 3. Read into dataframe, review schema
val yellowTaxiTripsBatchDF = spark.read.option("header", "true")
                      .schema(tripSchema)
                      .option("delimiter",",")
                      .csv("/mnt/workshop/staging/transactions/yellow_tripdata_2017-*.csv").toDF("vendor_id","pickup_datetime","dropoff_datetime","passenger_count","trip_distance","rate_code_id","store_and_fwd_flag","pickup_locn_id","dropoff_locn_id","payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount")

yellowTaxiTripsBatchDF.printSchema

// COMMAND ----------

// 4. Profile
display(yellowTaxiTripsBatchDF.describe())

// COMMAND ----------

// 5. Review
display(yellowTaxiTripsBatchDF)

// COMMAND ----------

// 6. Count
yellowTaxiTripsBatchDF.count

// COMMAND ----------

//7.  Persist as parquet

//Define destination and clean up
val  dbfsDestinationDirectory = "/mnt/workshop/raw/transactional-data/trips/"
dbutils.fs.rm(dbfsDestinationDirectory, recurse=true)

//Persist
yellowTaxiTripsBatchDF.coalesce(2).write.parquet(dbfsDestinationDirectory)

// COMMAND ----------

// 8. Create table

// COMMAND ----------

// MAGIC %sql 
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC 
// MAGIC USE taxi_db;
// MAGIC DROP TABLE IF EXISTS trips_raw;
// MAGIC CREATE TABLE IF NOT EXISTS trips_raw
// MAGIC USING parquet
// MAGIC LOCATION '/mnt/workshop/raw/transactional-data/trips/';
// MAGIC 
// MAGIC ANALYZE TABLE taxi_zone_lookup COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_db.trips_raw

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from taxi_db.trips_raw

// COMMAND ----------

