// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 3 of 3 notebooks that demonstrate stream ingest from Kafka, of live telemetry from Azure IoT hub.<BR>
// MAGIC - In notebook 1, we ingested from Kafka into Azure Cosmos DB SQL API (OLTP store)<BR>
// MAGIC - In notebook 2, we ingested from Kafka using structured stream processing and persisted to a Databricks Delta table (analytics store)<BR>
// MAGIC - In **this notebook**, we will run queries against the Delta table<BR>

// COMMAND ----------

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
// MAGIC OPTIMIZE telemetry_db.telemetry_delta_stream;
// MAGIC select max(temperature), max(humidity), max(pressure) from telemetry_db.telemetry_delta_stream;
// MAGIC --MIN: 71, 66, 135
// MAGIC --MAX: 79, 73, 164

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC OPTIMIZE telemetry_delta_stream;
// MAGIC select device_id,count(*) as telemetry_count from telemetry_db.telemetry_delta_stream group by device_id;

// COMMAND ----------

// MAGIC %sql
// MAGIC select device_id,avg(temperature) from telemetry_db.telemetry_delta_stream group by device_id;

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE telemetry_db.telemetry_delta_stream;
// MAGIC select max(temperature) from telemetry_db.telemetry_delta_stream where device_id='chiller-01.168'