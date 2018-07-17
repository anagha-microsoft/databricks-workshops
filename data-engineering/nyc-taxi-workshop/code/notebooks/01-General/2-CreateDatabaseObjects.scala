// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC 1) Database definition<BR> 
// MAGIC 2) External remote JDBC table definition

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Create the taxi_db database

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.1. Create database

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;

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
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_reports_db;
// MAGIC USE taxi_reports_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS BATCH_JOB_HISTORY;
// MAGIC CREATE TABLE BATCH_JOB_HISTORY
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://<yourDatabaseServer>.database.windows.net:1433;database=<yourDatabase>;',
// MAGIC   dbtable 'BATCH_JOB_HISTORY',
// MAGIC   user '<yourUserID>',
// MAGIC   password "<yourPassword>"
// MAGIC );

// COMMAND ----------

