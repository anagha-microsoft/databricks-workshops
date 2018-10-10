// Databricks notebook source
// MAGIC %md
// MAGIC ### 4. Create the Databricks delta table
// MAGIC This is a one-time activity that can only be run after a dataset has landed in the Delta table designated location.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS telemetry_db;
// MAGIC 
// MAGIC USE telemetry_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS telemetry_delta_stream;
// MAGIC CREATE TABLE telemetry_delta_stream
// MAGIC USING DELTA
// MAGIC LOCATION '/mnt/data/iot/rawDir/telemetry/';
// MAGIC 
// MAGIC OPTIMIZE telemetry_delta_stream;

// COMMAND ----------

// MAGIC %sql
// MAGIC describe telemetry_db.telemetry_delta_stream;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Query the Databricks delta table
// MAGIC You need to the run the optimize command each time you want to query, as the Delta table is continuously receiving data.  This command will ensure the physical partitions match the metastore partitions to get you the latest picture of your data.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC OPTIMIZE telemetry_delta_stream;
// MAGIC select * from telemetry_db.telemetry_delta_stream;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC OPTIMIZE telemetry_delta_stream;
// MAGIC select device_id,count(*) as telemetry_count from telemetry_db.telemetry_delta_stream group by device_id;

// COMMAND ----------

// MAGIC %sql
// MAGIC select device_id,avg(temperature) from telemetry_db.telemetry_delta_stream group by device_id;