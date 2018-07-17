// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC 1) Database definition<BR> 
// MAGIC 2) External remote JDBC table definition

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Create the flight_db database

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.1. Create database

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS flight_db;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.2. Validate

// COMMAND ----------

spark.catalog.listDatabases.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Create external table definition for a remote Azure SQL database table over JDBC
// MAGIC 
// MAGIC ###### TODO: secure credentials

// COMMAND ----------

// MAGIC %sql
// MAGIC --This table is in Azure SQL Database
// MAGIC DROP TABLE IF EXISTS flight_db.predictions_needed;
// MAGIC CREATE TABLE flight_db.predictions_needed
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://bhoomidbsrvr.database.windows.net:1433;database=bhoomidb',
// MAGIC   dbtable 'predictions_needed',
// MAGIC   user 'akhanolk',
// MAGIC   password 'A1rawat#123'
// MAGIC )

// COMMAND ----------

