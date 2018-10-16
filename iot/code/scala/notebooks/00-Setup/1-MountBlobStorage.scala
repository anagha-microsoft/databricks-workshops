// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC Learn how to mount various blob storage container directories so you can access them easily in your code, without storage details in each notebook.<br>
// MAGIC This is a one time activity.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Pre-requisites:
// MAGIC 1.  Create a blob storage account from the portal and give it a meaningful name<br>
// MAGIC 2.  Navigate into the blob storage and create the following containers with "private(no anonymous access)" configuration:<br>
// MAGIC     - raw - Persistence zone for raw data in full fidelity from source
// MAGIC     - curated - Peristence zone for transformed, deduped, curated datasets in optimized storage and query-efficent formats and partitioned for optimal querying
// MAGIC     - consumption - Persistence zone for datasets purpose-built for consumption; Materialized denomlalized views, canned reports etc
// MAGIC     - Note: Typically, you also have reference data - not in scope for this workshop

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Define credentials
// MAGIC We defined storage account credentials at the cluster level to work with the database UI of Azure databricks.<BR>
// MAGIC Here we need it to mount blob storage.

// COMMAND ----------

val storageAccount = "telemetryadbsa"
val storageAccountKey= spark.conf.get("spark.hadoop.fs.azure.account.key." + storageAccount + ".blob.core.windows.net")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Mount blob storage

// COMMAND ----------

// MAGIC %md
// MAGIC ######3.1.1. Define reusable function to mount blob storage

// COMMAND ----------

//This is a function to mount a storage container
def mountStorageContainer(storageAccount: String, storageAccountKey: String, storageContainer: String, blobMountPoint: String)
{
   try {
     
     println(s"Mounting ${storageContainer} to ${blobMountPoint}:")
    // Unmount the storage container if already mounted
    dbutils.fs.unmount(blobMountPoint)

  } catch { 
    //If this errors, the container is not mounted
    case e: Throwable => println(s"....Container is not mounted; Attempting mounting now..")

  } finally {
    // Mount the storage container
    val mountStatus = dbutils.fs.mount(
    source = "wasbs://" + storageContainer + "@" + storageAccount + ".blob.core.windows.net/",
    mountPoint = blobMountPoint,
    extraConfigs = Map("fs.azure.account.key." + storageAccount + ".blob.core.windows.net" -> storageAccountKey))
  
    println("...Status of mount is: " + mountStatus)
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ######3.1.2. Mount the storage containers defined under /mnt/data/movielens root directory path
// MAGIC This is a one-time activity

// COMMAND ----------

mountStorageContainer(storageAccount,storageAccountKey,"raw","/mnt/data/iot/rawDir")
mountStorageContainer(storageAccount,storageAccountKey,"curated","/mnt/data/iot/curatedDir")
mountStorageContainer(storageAccount,storageAccountKey,"consumption","/mnt/data/iot/consumptionDir")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. List storage containers mounted

// COMMAND ----------

display(dbutils.fs.ls("/mnt/data/iot/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ###5. Other

// COMMAND ----------

//5.1. Should you need to refresh
//dbutils.fs.refreshMounts()


//5.2. Unmount
//dbutils.fs.unmount("<yourMountPoint>")