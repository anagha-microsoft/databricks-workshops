// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC Learn how to mount various blob storage container directories so you can access them easily in your code, without defining credentials and wasbs URIs in each notebook

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Define credentials
// MAGIC We defined storage account credentials at the cluster level to work with the database UI of Azure databricks.<BR>
// MAGIC Here we need it to mount blob storage

// COMMAND ----------

val storageAccount = "bhoomisa"
val storageAccountKey= spark.conf.get("spark.hadoop.fs.azure.account.key." + storageAccount + ".blob.core.windows.net")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Mount blob storage

// COMMAND ----------

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

mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-staging","/mnt/data/workshop/stagingDir/nyctaxi")
mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-ref-data","/mnt/data/workshop/referenceDataDir/nyctaxi")
mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-raw","/mnt/data/workshop/rawDir/nyctaxi")
mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-curated","/mnt/data/workshop/curatedDir")
mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-consumption","/mnt/data/workshop/consumptionDir/nyctaxi")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. List storage containers mounted

// COMMAND ----------

display(dbutils.fs.ls("/mnt/data/workshop/*/nyctaxi"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Unmount blob storage - how to

// COMMAND ----------

//Unmount for notebook execution 
/*
dbutils.fs.unmount("/mnt/data/workshop/")
dbutils.fs.unmount("/mnt/data/workshop/refDataDir")
dbutils.fs.unmount("/mnt/data/workshop/rawDir")
dbutils.fs.unmount("/mnt/data/workshop/curatedDir")
dbutils.fs.unmount("/mnt/data/workshop/consumptionDir")

*/

// COMMAND ----------

//Refresh mounts
dbutils.fs.refreshMounts()

// COMMAND ----------

//List
display(dbutils.fs.ls("/mnt/data/nyctaxi/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Refresh mount points

// COMMAND ----------

//Refresh mounts
dbutils.fs.refreshMounts()

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/data/nyctaxi/