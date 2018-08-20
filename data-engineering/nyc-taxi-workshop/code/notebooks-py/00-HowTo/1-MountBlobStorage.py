# Databricks notebook source
# MAGIC %md
# MAGIC # Mount blob storage
# MAGIC 
# MAGIC Mounting blob storage containers in Azure Databricks allows you to access blob storage containers like they are directories.<BR>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define credentials
# MAGIC To mount blob storage - we need storage credentials - storage account name and storage account key

# COMMAND ----------

storageAccount = "<<your_storage_account>>"
storageAccountConfKey = "fs.azure.account.key." + storageAccount + ".blob.core.windows.net"
storageAccountConfVal= spark.conf.get("spark.hadoop.fs.azure.account.key." + storageAccount + ".blob.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Mount blob storage

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://nyctaxi-scratch@{}.blob.core.windows.net/".format(storageAccount),
  mount_point = "/mnt/<userid>/data/nyctaxi/scratchDir",
  extra_configs = {storageAccountConfKey: storageAccountConfVal})

# COMMAND ----------

# Display directories
display(dbutils.fs.ls("/mnt/<userid>/data/nyctaxi/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Refresh mount points

# COMMAND ----------

# Refresh mounts if applicable
dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. How to unmount

# COMMAND ----------

dbutils.fs.unmount("<yourMountPoint>")