// Databricks notebook source
// MAGIC %md
// MAGIC # Mount blob storage
// MAGIC 
// MAGIC Mounting blob storage containers in Azure Databricks allows you to access blob storage containers like they are directories.<BR>
// MAGIC   
// MAGIC ### What's in this exercise?
// MAGIC You will mount storage account containers required for the workshop primer section.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Define credentials
// MAGIC To mount blob storage - we need storage credentials - storage account name and storage account key

// COMMAND ----------

//Replace with your storage account name
val storageAccountName = "gwsblobsa"
val storageAccountAccessKey = dbutils.secrets.get(scope = "gws-blob-storage", key = "storage-acct-key")

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs /mnt/workshop

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Mount blob storage

// COMMAND ----------

/*
dbutils.fs.mount(
  source = "wasbs://scratch@generalworkshopsa.blob.core.windows.net/",
  mountPoint = "/mnt/workshop/scratch",
  extraConfigs = Map(storageAccountName -> storageAccountAccessKey))
*/

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

mountStorageContainer(storageAccountName,storageAccountAccessKey,"demo","/mnt/workshop/demo")
mountStorageContainer(storageAccountName,storageAccountAccessKey,"scratch","/mnt/workshop/scratch")
mountStorageContainer(storageAccountName,storageAccountAccessKey,"staging","/mnt/workshop/staging")
mountStorageContainer(storageAccountName,storageAccountAccessKey,"raw","/mnt/workshop/raw")
mountStorageContainer(storageAccountName,storageAccountAccessKey,"curated","/mnt/workshop/curated")
mountStorageContainer(storageAccountName,storageAccountAccessKey,"consumption","/mnt/workshop/consumption")

// COMMAND ----------

//Display directories
display(dbutils.fs.ls("/mnt/workshop"))

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

//dbutils.fs.unmount(<yourMountPoint>)
