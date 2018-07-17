// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC Learn how to create Hive tables on top of files in DBFS and query them<BR>
// MAGIC Inserting/overwriting etc are covered further in the workshop

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Create some sample data

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC # Create tmp directory
// MAGIC mkdir -p /tmp
// MAGIC cd /tmp
// MAGIC 
// MAGIC # Create a text file
// MAGIC cd /tmp
// MAGIC rm -rf us_states.csv
// MAGIC touch us_states.csv
// MAGIC 
// MAGIC # Add some content to the file 
// MAGIC echo "IL,Illinois" >> /tmp/us_states.csv 
// MAGIC echo "IN,Indiana" >> /tmp/us_states.csv 
// MAGIC echo "MN,Minnesota" >> /tmp/us_states.csv 
// MAGIC echo "WI,Wisconsin" >> /tmp/us_states.csv 

// COMMAND ----------

// MAGIC %sh
// MAGIC cat /tmp/us_states.csv 

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Save data to DBFS

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/data/mlw/scratchDir/

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs /mnt/data/mlw/scratchDir/testDir/hiveTableTest

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/data/mlw/scratchDir/testDir/

// COMMAND ----------

if (dbutils.fs.cp("file:/tmp/us_states.csv","/mnt/data/mlw/scratchDir/testDir/hiveTableTest"))
  dbutils.fs.rm("file:/tmp/us_states.csv",recurse=true)


// COMMAND ----------

dbutils.fs.ls("/mnt/data/mlw/scratchDir/testDir/hiveTableTest/")

// COMMAND ----------

// MAGIC %fs 
// MAGIC 
// MAGIC ls /tmp 

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Define a Hive table for US states

// COMMAND ----------

spark.catalog.listDatabases.show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS demo_db;

// COMMAND ----------

spark.catalog.listDatabases.show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC use demo_db;
// MAGIC 
// MAGIC drop table if exists us_states;
// MAGIC create external table if not exists us_states(
// MAGIC state_cd string,
// MAGIC state_nm string
// MAGIC )
// MAGIC row format delimited 
// MAGIC fields terminated by ','
// MAGIC location "/mnt/data/mlw/scratchDir/testDir/hiveTableTest/";

// COMMAND ----------

spark.catalog.setCurrentDatabase("demo_db")
spark.catalog.listTables.show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC use demo_db;
// MAGIC 
// MAGIC select * from us_states;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # References:
// MAGIC https://docs.databricks.com/spark/latest/spark-sql/language-manual/insert.html