// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 101 on working with Azure SQL database

// COMMAND ----------

// MAGIC %md
// MAGIC # 1.0. Azure SQL Database
// MAGIC 
// MAGIC Databricks clusters version 3.5 and above come with Azure SQL database/mysql and postgres drivers installed.
// MAGIC In this section, we will learn to work with Azure SQL database
// MAGIC 
// MAGIC The following is covered in this section-
// MAGIC 1. Querying a remote Azure SQL database table
// MAGIC 2. Reading from & writing to an Azure SQL database is covered as part of the lab

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS demo_db;
// MAGIC USE demo_db;
// MAGIC 
// MAGIC --This table is in Azure SQL Database; We are merely creating a schema on it
// MAGIC DROP TABLE IF EXISTS us_states;
// MAGIC CREATE TABLE us_states
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://demodbsrvr.database.windows.net:1433;database=demodb',
// MAGIC   dbtable 'us_states',
// MAGIC   user 'demodbadmin',
// MAGIC   password 'd@t@br1ck$'
// MAGIC )

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select * from demo_db.us_states where stateCode='IL'