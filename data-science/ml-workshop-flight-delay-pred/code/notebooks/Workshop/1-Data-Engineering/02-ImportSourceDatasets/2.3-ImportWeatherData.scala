// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC In this exercise, we will - <BR>
// MAGIC (1) Copy the source data for weather in the (transient) staging directory to the raw data directory<BR>
// MAGIC (2) Explore the source data for weather<BR>
// MAGIC (3) Review summary stats<BR>
// MAGIC   
// MAGIC Location of source data:<BR>
// MAGIC /mnt/data/mlw/stagingDir/weather-ref/AirportWeatherReferenceData.csv

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1. Copy source dataset from staging to raw data directory

// COMMAND ----------

//The source & destination directories
val sourceFile="/mnt/data/mlw/stagingDir/weather-ref/AirportWeatherReferenceData.csv"
val destinationDirectory="/mnt/data/mlw/rawDir/reference-data/weather/AirportWeatherReferenceData.csv"

// COMMAND ----------

//Check if source file is available
display(dbutils.fs.ls(sourceFile))

// COMMAND ----------

//Delete destination directory if it exists, for idempotency
dbutils.fs.rm(destinationDirectory,recurse=true)

// COMMAND ----------

//Save to the raw directory in full fidelity
dbutils.fs.cp(sourceFile,destinationDirectory)

// COMMAND ----------

//Check output
display(dbutils.fs.ls(destinationDirectory))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 2. Create Hive external table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC USE flight_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS weather_history;
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS weather_history(
// MAGIC year int,
// MAGIC month int,
// MAGIC day int,
// MAGIC time int,
// MAGIC time_zone int,
// MAGIC sky_condition string,
// MAGIC visibility int,
// MAGIC weather_type string,
// MAGIC dry_bulb_farenheit float,
// MAGIC dry_bulb_celsius float,
// MAGIC wet_bulb_farenheit float,
// MAGIC wet_bulb_celsius float,
// MAGIC dew_point_farenheit float,
// MAGIC dew_point_celsius float,
// MAGIC relative_humidity int,
// MAGIC wind_speed string, --is float, deliberately using string here
// MAGIC wind_direction int,
// MAGIC value_for_wind_character int,
// MAGIC station_pressure float,
// MAGIC pressure_tendency int,
// MAGIC pressure_change int,
// MAGIC sea_level_pressure string,--is float, deliberately using string here
// MAGIC record_type string,
// MAGIC hourly_precip string,--is float, deliberately using string here
// MAGIC altimeter float,
// MAGIC latitude double,
// MAGIC longitude double
// MAGIC )
// MAGIC Row format delimited
// MAGIC fields terminated by ','
// MAGIC LOCATION '/mnt/data/mlw/stagingDir/weather-ref/';

// COMMAND ----------

spark.catalog.setCurrentDatabase("flight_db")
spark.catalog.listTables.show(false)

// COMMAND ----------

//Refresh table
sql("REFRESH TABLE flight_db.weather_history")

//Run statistics on table for performance
sql("ANALYZE TABLE flight_db.weather_history COMPUTE STATISTICS")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 3. Explore

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 3.1. First few lines

// COMMAND ----------

//Read the first few lines
dbutils.fs.head(sourceFile)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 3.2. Query using SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from flight_db.weather_history

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 4. Summary and Descriptive Statistics

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 4.1. Profile the data with table stats 

// COMMAND ----------

val df = sqlContext.sql("""
select * from flight_db.weather_history
""").cache()

// COMMAND ----------

//Materialize 
df.count()

// COMMAND ----------

df.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 4.2. Profile the data with specific column stats 

// COMMAND ----------

df.describe("wind_speed","sea_level_pressure","hourly_precip").show()

// COMMAND ----------

