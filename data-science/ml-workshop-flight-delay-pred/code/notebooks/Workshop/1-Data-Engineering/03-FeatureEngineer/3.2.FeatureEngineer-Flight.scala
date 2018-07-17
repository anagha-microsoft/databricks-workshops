// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC In this exercise, we will - </BR>
// MAGIC (1) Profile the flight history data and identify cleansing/deduping/transformations to be completed</BR>
// MAGIC (2) Execute the transformations and persist a clean dataset

// COMMAND ----------

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration
import com.databricks.backend.daemon.dbutils.FileInfo

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1. Explore

// COMMAND ----------

// MAGIC %sql
// MAGIC describe formatted flight_db.flight_history

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from flight_db.flight_history;
// MAGIC --1048576

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1.1. Profile dep delay 15 attribute (flights delayed by 15 minutes or more)

// COMMAND ----------

sqlContext.sql("select dep_delay_15 from flight_db.flight_history").describe().show()

// COMMAND ----------

// MAGIC %sql
// MAGIC --6.1. Check for nulls
// MAGIC select count(*) from flight_db.flight_history where dep_delay_15 is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --6.2. Check for distinct values
// MAGIC select dep_delay_15,count(*) from flight_db.flight_history where group by dep_delay_15;

// COMMAND ----------

// MAGIC %sql
// MAGIC --6.3. Check for blanks
// MAGIC select count(*) from flight_db.flight_history where dep_delay_15='';

// COMMAND ----------

// MAGIC %sql
// MAGIC --6.4. Check for count of records with non-numeric values
// MAGIC select count(*) from flight_db.flight_history where cast(dep_delay_15 as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --6.5. Check for the non-numeric values
// MAGIC select distinct dep_delay_15 from flight_db.flight_history where cast(dep_delay_15 as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ** Transformations/cleansing identified: **
// MAGIC 
// MAGIC <li>Remove blanks</li>
// MAGIC <li>Dedupe</li>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 2. Transform

// COMMAND ----------

val flightDF = sqlContext.sql("""
select distinct * from (
select 
origin_airport_cd,
month,
day_of_month,
day_of_week,
round(crs_dep_tm/100,0) as dep_hour,
carrier_cd,
dest_airport_cd,
dep_delay_15
from flight_db.flight_history
where 
dep_delay_15 is not null) x
""").cache()

// COMMAND ----------

//Materialize
flightDF.count()

// COMMAND ----------

flightDF.printSchema

// COMMAND ----------

//Check record count to see duplicates excluded
flightDF.count()
//Original = 1,048,576
//Nulls = 12,100
//Cleansed and deduped = 1,023,352

// COMMAND ----------

flightDF.take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 3. Persist

// COMMAND ----------

//Persist cleansed, deduped data to curated zone
val destinationDirectory = "/mnt/data/mlw/curatedDir/flight"

//Delete output from prior execution
dbutils.fs.rm(destinationDirectory,recurse=true)

// COMMAND ----------

//Persist
flightDF.coalesce(1).write.option("delimiter",",").csv(destinationDirectory)

// COMMAND ----------

//Check destination directory
display(dbutils.fs.ls(destinationDirectory))

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destinationDirectory + "/").foreach((i: FileInfo) => if (!(i.path contains "csv")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 4. Create Hive external table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC USE flight_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS flight_history_curated;
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS flight_history_curated(
// MAGIC origin_airport_cd varchar(5),
// MAGIC month int,
// MAGIC day_of_month int,
// MAGIC day_of_week int,
// MAGIC dep_hour int,
// MAGIC carrier_cd varchar(50),
// MAGIC dest_airport_cd varchar(5),
// MAGIC dep_delay_15 int
// MAGIC )
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY ','
// MAGIC LOCATION '/mnt/data/mlw/curatedDir/flight';

// COMMAND ----------

//Refresh table
sql("REFRESH TABLE flight_db.flight_history_curated")

//Run statistics on table for performance
sql("ANALYZE TABLE flight_db.flight_history_curated COMPUTE STATISTICS")

// COMMAND ----------

// MAGIC %sql
// MAGIC --Count records 
// MAGIC select count(*) from flight_db.flight_history_curated

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from flight_db.flight_history_curated