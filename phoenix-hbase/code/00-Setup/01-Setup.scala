// Databricks notebook source
// MAGIC %md
// MAGIC ### Step 1: Mount storage container to use for DBFS

// COMMAND ----------

val storageAccount = "bhoomisa"
val storageAccountKey= spark.conf.get("spark.hadoop.fs.azure.account.key." + storageAccount + ".blob.core.windows.net")

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

mountStorageContainer(storageAccount,storageAccountKey,"movielens-staging","/mnt/data/movielens/stagingDir")
mountStorageContainer(storageAccount,storageAccountKey,"movielens-raw","/mnt/data/movielens/rawDir")

// COMMAND ----------

//List dirs
display(dbutils.fs.ls("/mnt/data/movielens/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Download the movielens public dataset, unzip and copy to DBFS
// MAGIC Use wget to download the dataset from http://files.grouplens.org/datasets/movielens/ml-1m.zip

// COMMAND ----------

// MAGIC %sh wget http://files.grouplens.org/datasets/movielens/ml-1m.zip -O /tmp/movielens.zip

// COMMAND ----------

// MAGIC %sh unzip /tmp/movielens.zip -d /tmp/movielens

// COMMAND ----------

// MAGIC %sh ls /tmp/movielens/ml-1m/

// COMMAND ----------

//Copy to staging directory
dbutils.fs.cp("file:/tmp/movielens/ml-1m/movies.dat", "/mnt/data/movielens/stagingDir/")
dbutils.fs.cp("file:/tmp/movielens/ml-1m/ratings.dat", "/mnt/data/movielens/stagingDir/")
dbutils.fs.cp("file:/tmp/movielens/ml-1m/users.dat", "/mnt/data/movielens/stagingDir/")
dbutils.fs.cp("file:/tmp/movielens/ml-1m/README", "/mnt/data/movielens/stagingDir/")

//Copy those that may not get parsed also to raw directory
dbutils.fs.cp("file:/tmp/movielens/ml-1m/users.dat", "/mnt/data/movielens/rawDir/")

// COMMAND ----------

//List files
display(dbutils.fs.ls("/mnt/data/movielens/stagingDir"))

// COMMAND ----------

// MAGIC %sh 
// MAGIC rm -r /tmp/movielens
// MAGIC rm -r /tmp/movielens.zip

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Create movie lens database

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC drop database if exists movielens_db; 
// MAGIC create database movielens_db;