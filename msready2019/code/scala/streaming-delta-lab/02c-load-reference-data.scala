// Databricks notebook source
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}

//1.  Schema for taxi zone lookup
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

display(refDF)

// COMMAND ----------

//3.  Persist as parquet
refDF.coalesce(1).write.parquet("/mnt/workshop/curated/reference-data/taxi-zone/")

// COMMAND ----------

//4. Create Hive external table

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

//5.  Explore

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_db.taxi_zone_lookup