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

val storageAccountName = "fs.azure.account.key.generalworkshopsa.blob.core.windows.net"
val storageAccountAccessKey = dbutils.secrets.get(scope = "bhoomi-storage", key = "storage-acct-key")

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs /mnt/data/workshop

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Mount blob storage

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://staging@generalworkshopsa.blob.core.windows.net/",
  mountPoint = "/mnt/data/workshop/stagingDir",
  extraConfigs = Map(storageAccountName -> storageAccountAccessKey))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://scratch@generalworkshopsa.blob.core.windows.net/",
  mountPoint = "/mnt/data/workshop/scratchDir",
  extraConfigs = Map(storageAccountName -> storageAccountAccessKey))

// COMMAND ----------

//Display directories
display(dbutils.fs.ls("/mnt/data/workshop"))

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