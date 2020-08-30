# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC 
# MAGIC Learn how to mount various blob storage container directories so you can access them easily in your code, without defining credentials and wasbs URIs in each notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define credentials
# MAGIC We defined storage account credentials at the cluster level to work with the database UI of Azure databricks.<BR>
# MAGIC Here we need it to mount blob storage

# COMMAND ----------

storageAccount = "<your_storage_account>"
storageAccountKey= spark.conf.get("spark.hadoop.fs.azure.account.key." + storageAccount + ".blob.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Mount blob storage

# COMMAND ----------

def mountStorageContainer(storageAccount, storageAccountKey, storageContainer, blobMountPoint):
  mountStatus = dbutils.fs.mount(
    source = "wasbs://" + storageContainer + "@" + storageAccount + ".blob.core.windows.net/",
    mount_point = blobMountPoint,
    extra_configs = {"fs.azure.account.key." + storageAccount + ".blob.core.windows.net": storageAccountKey})
  print("Status of mount of container " + storageContainer + " is: " + str(mountStatus))


# COMMAND ----------

mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-staging","/mnt/<userid>/data/nyctaxi/stagingDir/")
mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-ref-data","/mnt/<userid>/data/nyctaxi/referenceDataDir")
mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-raw","/mnt/<userid>/data/nyctaxi/rawDir")
mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-curated","/mnt/<userid>/data/nyctaxi/curatedDir")
mountStorageContainer(storageAccount,storageAccountKey,"nyctaxi-consumption","/mnt/<userid>/data/nyctaxi/consumptionDir")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. List storage containers mounted

# COMMAND ----------

display(dbutils.fs.ls("/mnt/<userid>/data/nyctaxi"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Unmount blob storage - how to

# COMMAND ----------

# Unmount for notebook execution 

# .fs.unmount("/mnt/<userid>/data/nyctaxi/stagingDir")
# dbutils.fs.unmount("/mnt/<userid>/data/nyctaxi/refDataDir")
# dbutils.fs.unmount("/mnt/<userid>/data/nyctaxi/rawDir")
# dbutils.fs.unmount("/mnt/<userid>/data/nyctaxi/curatedDir")
# dbutils.fs.unmount("/mnt/<userid>/data/nyctaxi/consumptionDir")


# COMMAND ----------

# Refresh mounts
dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/<userid>/data/nyctaxi/