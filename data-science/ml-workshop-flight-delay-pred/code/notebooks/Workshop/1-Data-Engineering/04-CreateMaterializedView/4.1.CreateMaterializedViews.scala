// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC In this exercise, we will join all 4 datasets - flight history curated, weather history curated, carrier master and airport master data to create a denormalized dataset for our modeling exercise

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1. Create materialized view

// COMMAND ----------

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration
import com.databricks.backend.daemon.dbutils.FileInfo

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 1.1. Explore

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC use flight_db;
// MAGIC 
// MAGIC select distinct  
// MAGIC fhc.dep_delay_15,
// MAGIC amo.airport_id as origin_airport_id,
// MAGIC fhc.origin_airport_cd,
// MAGIC amo.airport_nm as origin_airport_nm,
// MAGIC amo.airport_lat as origin_airport_latitude,
// MAGIC amo.airport_long as origin_airport_longitude,
// MAGIC fhc.month as flight_month,
// MAGIC fhc.day_of_month as flight_day_of_month,
// MAGIC fhc.day_of_week as flight_day_of_week,
// MAGIC fhc.dep_hour as flight_dep_hour, 
// MAGIC cm.carrier_indx,
// MAGIC fhc.carrier_cd,
// MAGIC cm.carrier_nm,
// MAGIC amd.airport_id as dest_airport_id,
// MAGIC fhc.dest_airport_cd,
// MAGIC amd.airport_nm as dest_airport_nm,
// MAGIC amd.airport_lat as dest_airport_latitude,
// MAGIC amd.airport_long as dest_airport_longitude,
// MAGIC whc.wind_speed,
// MAGIC whc.sea_level_pressure,
// MAGIC whc.hourly_precip
// MAGIC from 
// MAGIC flight_history_curated fhc
// MAGIC left outer join 
// MAGIC carrier_master cm
// MAGIC on (fhc.carrier_cd = cm.carrier_cd)
// MAGIC left outer join
// MAGIC airport_master amo
// MAGIC on (fhc.origin_airport_cd=amo.airport_cd)
// MAGIC left outer join
// MAGIC airport_master amd
// MAGIC on (fhc.dest_airport_cd=amd.airport_cd)
// MAGIC left outer join
// MAGIC weather_history_curated whc
// MAGIC on (whc.latitude=amo.airport_lat AND
// MAGIC     whc.longitude=amo.airport_long AND
// MAGIC     fhc.day_of_month=whc.day AND
// MAGIC     fhc.month=whc.month AND
// MAGIC     fhc.dep_hour=whc.hour)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 1.2. Create

// COMMAND ----------

val matViewDF = sqlContext.sql("""
select distinct  
fhc.dep_delay_15,
amo.airport_id as origin_airport_id,
fhc.origin_airport_cd,
amo.airport_nm as origin_airport_nm,
amo.airport_lat as origin_airport_latitude,
amo.airport_long as origin_airport_longitude,
fhc.month as flight_month,
fhc.day_of_month as flight_day_of_month,
fhc.day_of_week as flight_day_of_week,
fhc.dep_hour as flight_dep_hour, 
cm.carrier_indx,
fhc.carrier_cd,
cm.carrier_nm,
amd.airport_id as dest_airport_id,
fhc.dest_airport_cd,
amd.airport_nm as dest_airport_nm,
amd.airport_lat as dest_airport_latitude,
amd.airport_long as dest_airport_longitude,
whc.wind_speed,
whc.sea_level_pressure,
whc.hourly_precip
from 
flight_history_curated fhc
left outer join 
carrier_master cm
on (fhc.carrier_cd = cm.carrier_cd)
left outer join
airport_master amo
on (fhc.origin_airport_cd=amo.airport_cd)
left outer join
airport_master amd
on (fhc.dest_airport_cd=amd.airport_cd)
left outer join
weather_history_curated whc
on (whc.latitude=amo.airport_lat AND
    whc.longitude=amo.airport_long AND
    fhc.day_of_month=whc.day AND
    fhc.month=whc.month AND
    fhc.dep_hour=whc.hour)
""").cache()

// COMMAND ----------

//Materialize
matViewDF.count

// COMMAND ----------

//Print schema
matViewDF.printSchema

// COMMAND ----------

matViewDF.take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 1.3. Persist

// COMMAND ----------

//Destination diectory
val destinationDirectory="/mnt/data/mlw/consumptionDir/matView"

// COMMAND ----------

//Delete output from prior execution
dbutils.fs.rm(destinationDirectory,recurse=true)

// COMMAND ----------

//Persist
matViewDF.coalesce(1).write.option("delimiter",",").csv(destinationDirectory)

// COMMAND ----------

//Check destination directory
display(dbutils.fs.ls(destinationDirectory))

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destinationDirectory + "/").foreach((i: FileInfo) => if (!(i.path contains "csv")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 2. Create Hive external table

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 2.1. Create table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC USE flight_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS materialized_view;
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS materialized_view(
// MAGIC dep_delay_15 int,
// MAGIC origin_airport_id  int,
// MAGIC origin_airport_cd string,
// MAGIC origin_airport_nm string,
// MAGIC origin_airport_latitude double,
// MAGIC origin_airport_longitude double,
// MAGIC flight_month int,
// MAGIC flight_day_of_month int,
// MAGIC flight_day_of_week int,
// MAGIC flight_dep_hour int,
// MAGIC carrier_indx int,
// MAGIC carrier_cd string,
// MAGIC carrier_nm string,
// MAGIC dest_airport_id int,
// MAGIC dest_airport_cd string,
// MAGIC dest_airport_nm string,
// MAGIC dest_airport_latitude double,
// MAGIC dest_airport_longitude double,
// MAGIC wind_speed decimal(5,3),
// MAGIC sea_level_pressure decimal(5,3),
// MAGIC hourly_precip decimal(5,3) 
// MAGIC )
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY ','
// MAGIC LOCATION '/mnt/data/mlw/consumptionDir/matView';

// COMMAND ----------

spark.catalog.setCurrentDatabase("flight_db")
spark.catalog.listTables.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 2.2. Refresh Hive table

// COMMAND ----------

//Refresh table
sql("REFRESH TABLE flight_db.materialized_view")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 2.3. Compute statistics for query performance

// COMMAND ----------

//Refresh table
sql("ANALYZE TABLE flight_db.materialized_view COMPUTE STATISTICS")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 2.4. Query the table

// COMMAND ----------

// MAGIC %sql
// MAGIC use flight_db;
// MAGIC select * from materialized_view 