// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC For our model, we will focus on predicting delays using Wind_Speed (in MPH), 
// MAGIC Sea_Level_Pressure (in inches of Hg), and Hourly_Precip (in inches) 
// MAGIC ONLY, out of the 29 weather attributes in the source dataset
// MAGIC 
// MAGIC In this exercise, we will - </BR>
// MAGIC (1) Profile the weather data and identify cleansing/deduping/transformations to be completed</BR>
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
// MAGIC describe formatted flight_db.weather_history

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from flight_db.weather_history;
// MAGIC --406,516

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1.1. Profile wind speed

// COMMAND ----------

sqlContext.sql("select wind_speed from flight_db.weather_history").describe().show()

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 4.1. Check for nulls
// MAGIC select count(*) from flight_db.weather_history where wind_speed is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --4.2. Check for distinct values
// MAGIC select wind_speed,count(*) from flight_db.weather_history where group by wind_speed;

// COMMAND ----------

// MAGIC %sql
// MAGIC --4.3. Check for blanks
// MAGIC select count(*) from flight_db.weather_history where wind_speed='';

// COMMAND ----------

// MAGIC %sql
// MAGIC --4.4. Check for count of records with non-numeric values
// MAGIC select count(*) from flight_db.weather_history where cast(wind_speed as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --4.5. Check for the non-numeric values
// MAGIC select distinct wind_speed from flight_db.weather_history where cast(wind_speed as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ** Transformations/cleansing for wind speed: **
// MAGIC 
// MAGIC <li>Replace blanks with 0.0</li>
// MAGIC <li>Replace M for 'Missing' with 0.0</li>
// MAGIC <li>Dedupe</li>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1.2. Profile sea level pressure

// COMMAND ----------

sqlContext.sql("select sea_level_pressure from flight_db.weather_history").describe().show()

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 5.1. Check for nulls
// MAGIC select count(*) from flight_db.weather_history where sea_level_pressure is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 5.1. Check for nulls
// MAGIC select count(*) from flight_db.weather_history where sea_level_pressure is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --5.2. Check for distinct values
// MAGIC select sea_level_pressure,count(*) from flight_db.weather_history where group by sea_level_pressure;

// COMMAND ----------

// MAGIC %sql
// MAGIC --5.3. Check for blanks
// MAGIC select count(*) from flight_db.weather_history where sea_level_pressure='';

// COMMAND ----------

// MAGIC %sql
// MAGIC --5.4. Check for count of records with non-numeric values
// MAGIC select count(*) from flight_db.weather_history where cast(sea_level_pressure as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --5.5. Check for the non-numeric values
// MAGIC select distinct sea_level_pressure from flight_db.weather_history where cast(sea_level_pressure as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ** Transformations/cleansing for seal level pressure: **
// MAGIC 
// MAGIC <li>Replace blanks with 0.0</li>
// MAGIC <li>Replace M for 'Missing' with 29.2</li>
// MAGIC <li>Dedupe</li>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1.3. Profile hourly precipitation

// COMMAND ----------

// MAGIC %sql
// MAGIC -- 6.1. Check for nulls
// MAGIC select count(*) from flight_db.weather_history where hourly_precip is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --6.2. Check for distinct values
// MAGIC select hourly_precip,count(*) from flight_db.weather_history where group by hourly_precip;

// COMMAND ----------

// MAGIC %sql
// MAGIC --6.3. Check for blanks
// MAGIC select count(*) from flight_db.weather_history where hourly_precip='';

// COMMAND ----------

// MAGIC %sql
// MAGIC --6.4. Check for count of records with non-numeric values
// MAGIC select count(*) from flight_db.weather_history where cast(hourly_precip as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --6.5. Check for the non-numeric values
// MAGIC select distinct hourly_precip from flight_db.weather_history where cast(hourly_precip as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ** Transformations/cleansing for seal level pressure: **
// MAGIC 
// MAGIC <li>Replace blanks with 0.0</li>
// MAGIC <li>Replace T for trace amount of rain with 0.05</li>
// MAGIC <li>Dedupe</li>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 2. Transform

// COMMAND ----------

val weatherDF = sqlContext.sql("""
select distinct * from
(
select distinct 
latitude,
longitude,
month,
day,
ceil(time / 100) as hour,
cast((case wind_speed when '' then 0.0 when 'M' then 0.0 else wind_speed end) as decimal(5,3)) as wind_speed,
cast((case sea_level_pressure when '' then 0.0 when 'M' then 29.92 else sea_level_pressure end) as decimal(5,3)) as sea_level_pressure,
cast((case hourly_precip when '' then 0.0 when 'T' then 0.05 else hourly_precip end) as decimal(5,3)) as hourly_precip 
from 
flight_db.weather_history 
) x
""").cache()

// COMMAND ----------

//Materialize
weatherDF.count()

// COMMAND ----------

weatherDF.printSchema

// COMMAND ----------

//Check record count to see duplicates excluded
weatherDF.distinct.count()
//Original = 406,516
//Cleansed and deduped = 395,975


// COMMAND ----------

weatherDF.take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 3. Persist

// COMMAND ----------

//Determine output file size - we want the targeted output file size to be 64 MB
val fs = FileSystem.get(new Configuration())
val inputFileSize=fs.getContentSummary(new Path("/mnt/data/mlw/rawDir/reference-data/weather/AirportWeatherReferenceData.csv")).getLength 
var outputFileCount: Int = Math.floor((inputFileSize* 0.19) / (64 * 1024 * 1024)).toInt
if(outputFileCount == 0) outputFileCount=1 

// COMMAND ----------

//Persist cleansed, deduped data to curated zone
val destinationDirectory = "/mnt/data/mlw/curatedDir/weather"

//Delete output from prior execution
dbutils.fs.rm(destinationDirectory,recurse=true)

// COMMAND ----------

//Persist
weatherDF.coalesce(outputFileCount).write.option("delimiter",",").csv(destinationDirectory)

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
// MAGIC DROP TABLE IF EXISTS weather_history_curated;
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS weather_history_curated(
// MAGIC latitude double,
// MAGIC longitude double,
// MAGIC month int,
// MAGIC day int,
// MAGIC hour int,
// MAGIC wind_speed decimal(5,3),
// MAGIC sea_level_pressure decimal(5,3),
// MAGIC hourly_precip decimal(5,3) 
// MAGIC )
// MAGIC Row format delimited
// MAGIC fields terminated by ','
// MAGIC LOCATION '/mnt/data/mlw/curatedDir/weather/';

// COMMAND ----------

//Refresh table
sql("REFRESH TABLE flight_db.weather_history_curated")

//Run statistics on table for performance
sql("ANALYZE TABLE flight_db.weather_history_curated COMPUTE STATISTICS")

// COMMAND ----------

// MAGIC %sql
// MAGIC --Count records 
// MAGIC select count(*) from flight_db.weather_history_curated

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from flight_db.weather_history_curated