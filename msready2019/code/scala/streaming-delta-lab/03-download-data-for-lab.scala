// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this notebook:
// MAGIC We will download transactional data for the lab to driver local and then load to DBFS 

// COMMAND ----------

// 1.  Download datasets from instructor's storage account to driver local

// COMMAND ----------

// MAGIC %sh
// MAGIC cd /tmp
// MAGIC rm -rf nyclab-files
// MAGIC mkdir nyclab-files
// MAGIC cd nyclab-files
// MAGIC wget "https://ready2018adbsa.blob.core.windows.net/staging/taxi_zone_lookup.csv"
// MAGIC wget "https://ready2018adbsa.blob.core.windows.net/staging/yellow_tripdata_2017-01.csv"
// MAGIC wget "https://ready2018adbsa.blob.core.windows.net/staging/yellow_tripdata_2017-02.csv"

// COMMAND ----------

// 2.  List downloaded data in driver local
display(dbutils.fs.ls("file:/tmp/nyclab-files/"))

// COMMAND ----------

// 3.  Upload reference data to DBFS backed by Azure Blob Storage
dbutils.fs.rm("/mnt/workshop/staging/reference-data/",recurse=true)
dbutils.fs.mkdirs("/mnt/workshop/staging/reference-data/")
dbutils.fs.cp("file:/tmp/nyclab-files/taxi_zone_lookup.csv","/mnt/workshop/staging/reference-data/")
display(dbutils.fs.ls("/mnt/workshop/staging/reference-data/"))

// COMMAND ----------

// 4.  Upload transactional data to DBFS backed by Azure Blob Storage
dbutils.fs.rm("/mnt/workshop/staging/transactions/",recurse=true)
dbutils.fs.mkdirs("/mnt/workshop/staging/transactions/")
dbutils.fs.cp("file:/tmp/nyclab-files/yellow_tripdata_2017-01.csv","/mnt/workshop/staging/transactions/")
dbutils.fs.cp("file:/tmp/nyclab-files/yellow_tripdata_2017-02.csv","/mnt/workshop/staging/transactions/")
display(dbutils.fs.ls("/mnt/workshop/staging/transactions/"))