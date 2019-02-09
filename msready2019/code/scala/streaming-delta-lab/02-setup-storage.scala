// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this notebook?
// MAGIC We will mount blob storage containers - staging, raw, curated

// COMMAND ----------

// MAGIC %run ./01-common

// COMMAND ----------

// 1. Create root directory 
dbutils.fs.mkdirs("/mnt/workshop/")

// COMMAND ----------

// 2.  Function to unmount - for idempotency of repeated notebook execution
def unmount(mountPoint: String) =
  if (dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(mountPoint))
    dbutils.fs.unmount(mountPoint)

// In case already mounted, unmount
unmount("/mnt/workshop/staging")
unmount("/mnt/workshop/raw")
unmount("/mnt/workshop/curated")

// COMMAND ----------

// 3. Mount staging container
var containerName="staging"
dbutils.fs.mount(
  source = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/",
  mountPoint = "/mnt/workshop/" + containerName,
  extraConfigs = Map("fs.azure.account.key." + storageAccountName + ".blob.core.windows.net" -> storageAccountAccessKey))

// COMMAND ----------

// 4. Mount raw container
containerName="raw"
dbutils.fs.mount(
  source = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/",
  mountPoint = "/mnt/workshop/" + containerName,
  extraConfigs = Map("fs.azure.account.key." + storageAccountName + ".blob.core.windows.net" -> storageAccountAccessKey))

// COMMAND ----------

// 5. Mount curated container
containerName="curated"
dbutils.fs.mount(
  source = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/",
  mountPoint = "/mnt/workshop/" + containerName,
  extraConfigs = Map("fs.azure.account.key." + storageAccountName + ".blob.core.windows.net" -> storageAccountAccessKey))

// COMMAND ----------

// 6. List
display(dbutils.fs.ls("/mnt/workshop/"))