// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this notebook?
// MAGIC 1.  Learn to query a Databricks Delta table with data being streamed into it

// COMMAND ----------

// MAGIC %md
// MAGIC **Create table - this will fail unless data exists already in the "location"**  

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC 
// MAGIC USE taxi_db;
// MAGIC DROP TABLE IF EXISTS trips;
// MAGIC CREATE TABLE trips
// MAGIC USING DELTA
// MAGIC LOCATION "/mnt/workshop/curated/transactions/trips/";

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from trips limit 10;

// COMMAND ----------

// MAGIC %md
// MAGIC **Run this every few seconds and watch the count increase.**  

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from trips;

// COMMAND ----------

// MAGIC %md
// MAGIC **Now:** Go to 06a-stream-consume-basic notebook and cancel the query. <br>
// MAGIC Proceed to 07-stream-consume-advanced notebook.