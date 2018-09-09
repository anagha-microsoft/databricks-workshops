// Databricks notebook source
// MAGIC %md
// MAGIC # Mount blob storage
// MAGIC 
// MAGIC Mounting blob storage containers in Azure Databricks allows you to access blob storage containers like they are directories.<BR>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Define credentials
// MAGIC To mount blob storage - we need storage credentials - storage account name and storage account key

// COMMAND ----------

val storageAccount = "movielenssa"
val storageAccountConfKey = "fs.azure.account.key." + storageAccount + ".blob.core.windows.net"
val storageAccountConfVal= spark.conf.get("spark.hadoop.fs.azure.account.key." + storageAccount + ".blob.core.windows.net")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Mount blob storage

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://staging@movielenssa.blob.core.windows.net/",
  mountPoint = "/mnt/data/movielens/stagingDir",
  extraConfigs = Map(storageAccountConfKey -> storageAccountConfVal))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://staging@movielenssa.blob.core.windows.net/",
  mountPoint = "/mnt/data/movielens/scratchDir",
  extraConfigs = Map(storageAccountConfKey -> storageAccountConfVal))

// COMMAND ----------

//Display directories
display(dbutils.fs.ls("/mnt/data/movielens/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Refresh mount points

// COMMAND ----------

//Refresh mounts if applicable
//dbutils.fs.refreshMounts()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. How to unmount

// COMMAND ----------

//dbutils.fs.unmount("<yourMountPoint>")