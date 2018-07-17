// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC In this exercise, we will - <BR>
// MAGIC (1) Copy the source data for airports in the (transient) staging directory to the raw data directory<BR>
// MAGIC (2) Explore the source data for airports<BR>
// MAGIC (3) Review summary stats<BR>
// MAGIC 
// MAGIC Location of source data:<BR>
// MAGIC /mnt/data/mlw/stagingDir/airport-master/AirportMasterData.csv

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1. Copy source dataset from staging to raw data directory

// COMMAND ----------

//The source & destination directories
val sourceFile="/mnt/data/mlw/stagingDir/airport-master/AirportMasterData.csv"
val destinationDirectory="/mnt/data/mlw/rawDir/master-data/airport"

// COMMAND ----------

//Check if source file is available
display(dbutils.fs.ls(sourceFile))

// COMMAND ----------

//Delete destination directory if it exists, for idempotency
dbutils.fs.rm(destinationDirectory,recurse=true)

//Create destination directory
dbutils.fs.mkdirs(destinationDirectory)

//Save to the raw directory in full fidelity
dbutils.fs.cp(sourceFile,destinationDirectory)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 2. Create Hive external table

// COMMAND ----------

// MAGIC %sql
// MAGIC USE flight_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS carrier_master;
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS carrier_master(
// MAGIC carrier_indx INT,
// MAGIC carrier_cd STRING,
// MAGIC carrier_nm STRING
// MAGIC )
// MAGIC ROW FORMAT DELIMITED 
// MAGIC FIELDS TERMINATED BY ','
// MAGIC LOCATION '/mnt/data/mlw/rawDir/master-data/carrier';

// COMMAND ----------

spark.catalog.setCurrentDatabase("flight_db")
spark.catalog.listTables.show(false)

// COMMAND ----------

spark.catalog.setCurrentDatabase("flight_db")
spark.catalog.listTables.show(false)

// COMMAND ----------

//Refresh table
sql("REFRESH TABLE flight_db.airport_master")

//Run statistics on table for performance
sql("ANALYZE TABLE flight_db.airport_master COMPUTE STATISTICS")

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
// MAGIC select * from flight_db.airport_master

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
select * from flight_db.airport_master
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

df.describe("airport_id","airport_cd").show()