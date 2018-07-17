// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC 1) Read raw data, augment with derived attributes, augment with reference data & persist<BR> 
// MAGIC 2) Create external unmanaged Hive tables<BR>
// MAGIC 3) Create statistics for tables                          

// COMMAND ----------

import spark.implicits._
import spark.sql
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}

// COMMAND ----------

//Destination directory
val destDataDirRoot = "/mnt/data/nyctaxi/curatedDir/yellow-taxi" 

//Delete any residual data from prior executions for an idempotent run
dbutils.fs.rm(destDataDirRoot,recurse=true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.  Execute notebook with common/reusable functions 

// COMMAND ----------

// MAGIC %run "../01-General/3-CommonFunctions"

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.  Read raw, augment, persist as parquet 

// COMMAND ----------

val curatedDF = spark.sql("""
  select distinct t.taxi_type,
      t.vendor_id as vendor_id,
      t.pickup_datetime,
      t.dropoff_datetime,
      t.store_and_fwd_flag,
      t.rate_code_id,
      t.pickup_location_id,
      t.dropoff_location_id,
      t.pickup_longitude,
      t.pickup_latitude,
      t.dropoff_longitude,
      t.dropoff_latitude,
      t.passenger_count,
      t.trip_distance,
      t.fare_amount,
      t.extra,
      t.mta_tax,
      t.tip_amount,
      t.tolls_amount,
      t.improvement_surcharge,
      t.total_amount,
      t.payment_type,
      t.trip_year,
      t.trip_month,
      v.abbreviation as vendor_abbreviation,
      v.description as vendor_description,
      tm.month_name_short,
      tm.month_name_full,
      pt.description as payment_type_description,
      rc.description as rate_code_description,
      tzpu.borough as pickup_borough,
      tzpu.zone as pickup_zone,
      tzpu.service_zone as pickup_service_zone,
      tzdo.borough as dropoff_borough,
      tzdo.zone as dropoff_zone,
      tzdo.service_zone as dropoff_service_zone,
      year(t.pickup_datetime) as pickup_year,
      month(t.pickup_datetime) as pickup_month,
      day(t.pickup_datetime) as pickup_day,
      hour(t.pickup_datetime) as pickup_hour,
      minute(t.pickup_datetime) as pickup_minute,
      second(t.pickup_datetime) as pickup_second,
      date(t.pickup_datetime) as pickup_date,
      year(t.dropoff_datetime) as dropoff_year,
      month(t.dropoff_datetime) as dropoff_month,
      day(t.dropoff_datetime) as dropoff_day,
      hour(t.dropoff_datetime) as dropoff_hour,
      minute(t.dropoff_datetime) as dropoff_minute,
      second(t.dropoff_datetime) as dropoff_second,
      date(t.dropoff_datetime) as dropoff_date
  from 
    taxi_db.yellow_taxi_trips t
    left outer join taxi_db.vendor_lookup v 
      on (t.vendor_id = case when t.trip_year < "2015" then v.abbreviation else v.vendor_id end)
    left outer join taxi_db.trip_month_lookup tm 
      on (t.trip_month = tm.trip_month)
    left outer join taxi_db.payment_type_lookup pt 
      on (t.payment_type = case when t.trip_year < "2015" then pt.abbreviation else pt.payment_type end)
    left outer join taxi_db.rate_code_lookup rc 
      on (t.rate_code_id = rc.rate_code_id)
    left outer join taxi_db.taxi_zone_lookup tzpu 
      on (t.pickup_location_id = tzpu.location_id)
    left outer join taxi_db.taxi_zone_lookup tzdo 
      on (t.dropoff_location_id = tzdo.location_id)
  """)

val curatedDFConformed = curatedDF.withColumn("temp_vendor_id", col("vendor_id").cast(IntegerType)).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id").withColumn("temp_payment_type", col("payment_type").cast(IntegerType)).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type")

//Save as parquet, partition by year and month
curatedDFConformed.coalesce(15).write.partitionBy("trip_year", "trip_month").parquet(destDataDirRoot)

// COMMAND ----------

// MAGIC %md
// MAGIC With no tuning, and with no coalescing it created very tiny files and took 8.58 minutes for 2016 and 2017 -> 1.5 years <BR>
// MAGIC With coalesce(1) - it took 8.73 minutes but created 10 ~40 MB files per partition (year/month) <BR> <BR>
// MAGIC For the entire dataset - it took 27 minutes with 56GB RAM and 8 cores....

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
recursivelyDeleteSparkJobFlagFiles(destDataDirRoot)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.  Define Hive external table

// COMMAND ----------

// MAGIC %sql
// MAGIC USE taxi_db;
// MAGIC DROP TABLE IF EXISTS yellow_taxi_trips_curated;
// MAGIC CREATE TABLE yellow_taxi_trips_curated(
// MAGIC taxi_type STRING,
// MAGIC vendor_id INT,
// MAGIC pickup_datetime TIMESTAMP,
// MAGIC dropoff_datetime TIMESTAMP,
// MAGIC store_and_fwd_flag STRING,
// MAGIC rate_code_id INT,
// MAGIC pickup_location_id INT,
// MAGIC dropoff_location_id INT,
// MAGIC pickup_longitude STRING,
// MAGIC pickup_latitude STRING,
// MAGIC dropoff_longitude STRING,
// MAGIC dropoff_latitude STRING,
// MAGIC passenger_count INT,
// MAGIC trip_distance DOUBLE,
// MAGIC fare_amount DOUBLE,
// MAGIC extra DOUBLE,
// MAGIC mta_tax DOUBLE,
// MAGIC tip_amount DOUBLE,
// MAGIC tolls_amount DOUBLE,
// MAGIC improvement_surcharge DOUBLE,
// MAGIC total_amount DOUBLE,
// MAGIC payment_type INT,
// MAGIC trip_year STRING,
// MAGIC trip_month STRING,
// MAGIC vendor_abbreviation STRING,
// MAGIC vendor_description STRING,
// MAGIC month_name_short STRING,
// MAGIC month_name_full STRING,
// MAGIC payment_type_description STRING,
// MAGIC rate_code_description STRING,
// MAGIC pickup_borough STRING,
// MAGIC pickup_zone STRING,
// MAGIC pickup_service_zone STRING,
// MAGIC dropoff_borough STRING,
// MAGIC dropoff_zone STRING,
// MAGIC dropoff_service_zone STRING,
// MAGIC pickup_year INT,
// MAGIC pickup_month INT,
// MAGIC pickup_day INT,
// MAGIC pickup_hour INT,
// MAGIC pickup_minute INT,
// MAGIC pickup_second INT,
// MAGIC dropoff_year INT,
// MAGIC dropoff_month INT,
// MAGIC dropoff_day INT,
// MAGIC dropoff_hour INT,
// MAGIC dropoff_minute INT,
// MAGIC dropoff_second INT)
// MAGIC USING parquet
// MAGIC partitioned by (trip_year,trip_month)
// MAGIC LOCATION '/mnt/data/nyctaxi/curatedDir/yellow-taxi/';

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.  Create Hive table partitions

// COMMAND ----------

//Register Hive partitions for the transformed table
spark.sql("MSCK REPAIR TABLE taxi_db.yellow_taxi_trips_curated")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5.  Compute Hive table statistics

// COMMAND ----------

sql("REFRESH TABLE taxi_db.yellow_taxi_trips_curated")
sql("ANALYZE TABLE taxi_db.yellow_taxi_trips_curated COMPUTE STATISTICS")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6.  Explore

// COMMAND ----------

// MAGIC %sql
// MAGIC select trip_year,trip_month, count(*) as trip_count from taxi_db.yellow_taxi_trips_curated group by trip_year,trip_month
