// Databricks notebook source
// MAGIC %md
// MAGIC # Mount Azure Data Lake Store Gen2
// MAGIC 
// MAGIC Mounting Azure storage in Azure Databricks allows you to access the cloud storage like they are directories.<BR>
// MAGIC   
// MAGIC ### What's in this exercise?
// MAGIC The scope of this workshop is restricted to access via Service Principal and AAD based pass through authentication is out of scope. We will mount ADLSGen2 to Databricks in this module.<BR>
// MAGIC 
// MAGIC To mount ADLS Gen 2 - we need a few pieces of information-<BR>
// MAGIC 1.  Service principal - Application ID<BR>
// MAGIC 2.  Directory ID (AAD tenant ID)<BR>
// MAGIC 3.  Access tokey/key associated with the application ID<BR>
// MAGIC 4.  ADLSGen2 storage account key<BR>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Initialize root file system in ADLSGen2

// COMMAND ----------

spark.conf.set("fs.azure.account.key.gwsadlsgen2sa.dfs.core.windows.net", dbutils.secrets.get(scope = "gws-adlsgen2-storage", key = "storage-acct-key"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://gwsroot@gwsadlsgen2sa.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Define credentials for mounting

// COMMAND ----------

// Credentials
val clientID = dbutils.secrets.get(scope = "gws-adlsgen2-storage", key = "client-id")
val clientSecret = dbutils.secrets.get(scope = "gws-adlsgen2-storage", key = "client-secret")
val tenantID = "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = "gws-adlsgen2-storage", key = "tenant-id") + "/oauth2/token"

// ADLS config for mounting
val adlsConfigs = Map("fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> clientID,
  "fs.azure.account.oauth2.client.secret" -> clientSecret,
  "fs.azure.account.oauth2.client.endpoint" -> tenantID)

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs /mnt/workshop-adlsgen2

// COMMAND ----------

// MAGIC %md
// MAGIC Using storage explorer, grant the service principal name full access RWX to top level and child items with access and default ACLs

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Mount ADLSGen2 file systems

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.1. Mount a single file system

// COMMAND ----------

//dbutils.fs.unmount("/mnt/workshop-adlsgen2/gwsroot/")

// COMMAND ----------

// Sample for mounting gwsroot file system
dbutils.fs.mount(
  source = "abfss://gwsroot@gwsadlsgen2sa.dfs.core.windows.net/",
  mountPoint = "/mnt/workshop-adlsgen2/gwsroot/",
  extraConfigs = adlsConfigs)

// COMMAND ----------

// Check if already mounted
display(dbutils.fs.ls("/mnt/workshop-adlsgen2/gwsroot"))
/*
# Unmount if already mounted - as needed
dbutils.fs.unmount("/mnt/workshop/consumption/")
dbutils.fs.unmount("/mnt/workshop/curated/")
dbutils.fs.unmount("/mnt/workshop/demo/")
dbutils.fs.unmount("/mnt/workshop/raw/")
dbutils.fs.unmount("/mnt/workshop/staging/")
dbutils.fs.unmount("/mnt/workshop/scratch/")
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

//Mount the various storage containers created

//mountStorageContainer(storageAccountName,storageAccountAccessKey,"demo","/mnt/workshop/demo")
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
