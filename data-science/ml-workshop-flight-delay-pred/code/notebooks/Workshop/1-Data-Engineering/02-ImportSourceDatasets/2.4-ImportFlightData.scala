// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC In this exercise, we will - <BR>
// MAGIC (1) Copy the source data for flights in the (transient) staging directory to the raw data directory<BR>
// MAGIC (2) Explore the source data for flights<BR>
// MAGIC (3) Review summary stats<BR>
// MAGIC (4) Visualize<BR>
// MAGIC 
// MAGIC Location of source data:</BR>
// MAGIC /mnt/data/mlw/stagingDir/flight-transactions
// MAGIC lightDelaysWithAirportCodes-part[1-3].csv

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1. Copy source dataset from staging to raw data directory

// COMMAND ----------

//Source and destination directories
val sourceDirectory="/mnt/data/mlw/stagingDir/flight-transactions"
val destinationDirectory="/mnt/data/mlw/rawDir/transactional-data/flight"

// COMMAND ----------

//Check if source files are available
display(dbutils.fs.ls(sourceDirectory))

// COMMAND ----------

//Delete destination directory if it exists, for idempotency
dbutils.fs.rm(destinationDirectory,recurse=true)

// COMMAND ----------

//Save to the raw directory in full fidelity
dbutils.fs.cp(sourceDirectory,destinationDirectory, recurse=true)

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
// MAGIC DROP TABLE IF EXISTS flight_history;
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS flight_history(
// MAGIC year int,
// MAGIC month int,
// MAGIC day_of_month int,
// MAGIC day_of_week int,
// MAGIC carrier_cd string,
// MAGIC crs_dep_tm int,
// MAGIC dep_delay int,
// MAGIC dep_delay_15 int,
// MAGIC crs_arr_tm int,
// MAGIC arr_delay int,
// MAGIC arr_delay_15 int,
// MAGIC cancelled int,
// MAGIC origin_airport_cd string,
// MAGIC dest_airport_cd string
// MAGIC )
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY ','
// MAGIC LOCATION '/mnt/data/mlw/rawDir/transactional-data/flight';

// COMMAND ----------

spark.catalog.setCurrentDatabase("flight_db")
spark.catalog.listTables.show(false)

// COMMAND ----------

//Refresh table
sql("REFRESH TABLE flight_db.flight_history")

//Run statistics on table for performance
sql("ANALYZE TABLE flight_db.flight_history COMPUTE STATISTICS")

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
dbutils.fs.head(sourceDirectory + "/FlightDelaysWithAirportCodes-part3.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 3.2. Query using SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from flight_db.flight_history

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
select * from flight_db.flight_history
""").cache()

// COMMAND ----------

//Materialize 
df.count()

// COMMAND ----------

//Summary stats
df.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 4.2. Profile the data with specific column stats 

// COMMAND ----------

df.describe("dep_delay_15").show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 5. Visualization

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (
// MAGIC SELECT carrier_cd,count(dep_delay_15) as delay_count FROM 
// MAGIC flight_db.flight_history where dep_delay_15=1
// MAGIC group by carrier_cd
// MAGIC order by delay_count desc) x limit 10

// COMMAND ----------

display(sqlContext.sql("select case dep_delay_15 when 1 then 'delayed' else 'on-time' end delayed, count(dep_delay_15) from flight_db.flight_history where dep_delay_15 is not null group by dep_delay_15"))

// COMMAND ----------

// MAGIC %sql 
// MAGIC select month,count(dep_delay_15) as delayed_count from flight_db.flight_history where dep_delay_15=1 group by month

// COMMAND ----------

// MAGIC %sql 
// MAGIC select month,day_of_week,count(dep_delay_15) as delayed_count from flight_db.flight_history where dep_delay_15=1 group by month,day_of_week order by month,day_of_week

// COMMAND ----------

// MAGIC %sql 
// MAGIC select month,day_of_month,count(dep_delay_15) as delayed_count from flight_db.flight_history where dep_delay_15=1 group by month,day_of_month order by month,day_of_month

// COMMAND ----------

