// Databricks notebook source
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

// MAGIC %sql
// MAGIC --Explore data to be scored..Query the Azure SQL Server table from Spark, like its local
// MAGIC select * from flight_db.predictions_needed