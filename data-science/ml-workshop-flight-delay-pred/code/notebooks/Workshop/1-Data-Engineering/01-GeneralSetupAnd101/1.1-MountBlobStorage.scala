// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC Learn how to mount various blob storage container directories so you can access them easily in your code, without defining credentials and wasbs URIs in each notebook

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Define credentials
// MAGIC We defined storage account credentials at the cluster level to work with the database UI of Azure databricks
// MAGIC Here we need it to mount blob storage

// COMMAND ----------

val storageAccount = "bhoomisa"
val storageAccountKey= spark.conf.get("spark.hadoop.fs.azure.account.key." + storageAccount + "".blob.core.windows.net")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Mount blob storage

// COMMAND ----------

def mountStorageContainer(storageAccount: String, storageAccountKey: String, storageContainer: String, blobMountPoint: String)
{
  val mountStatus = dbutils.fs.mount(
  source = "wasbs://" + storageContainer + "@" + storageAccount + ".blob.core.windows.net/",
  mountPoint = blobMountPoint,
  extraConfigs = Map("fs.azure.account.key." + storageAccount + ".blob.core.windows.net" -> storageAccountKey))
  
  println("Status of mount of container " + storageContainer + " is: " + mountStatus)
}

// COMMAND ----------

mountStorageContainer(storageAccount,storageAccountKey,"mlw-scratchdir","/mnt/data/mlw/scratchDir")
mountStorageContainer(storageAccount,storageAccountKey,"mlw-staging","/mnt/data/mlw/stagingDir")
mountStorageContainer(storageAccount,storageAccountKey,"mlw-raw","/mnt/data/mlw/rawDir")
mountStorageContainer(storageAccount,storageAccountKey,"mlw-curated","/mnt/data/mlw/curatedDir")
mountStorageContainer(storageAccount,storageAccountKey,"mlw-consumption","/mnt/data/mlw/consumptionDir")
mountStorageContainer(storageAccount,storageAccountKey,"mlw-model","/mnt/data/mlw/modelDir")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. List storage containers mounted

// COMMAND ----------

display(dbutils.fs.ls("/mnt/data/mlw"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Unmount blob storage - how to

// COMMAND ----------

//Unmount for notebook execution 
/*
dbutils.fs.unmount("/mnt/data/mlw/stagingDir")
dbutils.fs.unmount("/mnt/data/mlw/rawDir")
dbutils.fs.unmount("/mnt/data/mlw/curatedDir")
dbutils.fs.unmount("/mnt/data/mlw/consumptionDir")
dbutils.fs.unmount("/mnt/data/mlw/scratchDir")
dbutils.fs.unmount("/mnt/data/mlw/modelDir")
*/

// COMMAND ----------

//Refresh mounts
dbutils.fs.refreshMounts()

// COMMAND ----------

//List
display(dbutils.fs.ls("/mnt/data/mlw/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Refresh mount points - how to

// COMMAND ----------

//Refresh mounts
dbutils.fs.refreshMounts()

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/data/mlw/