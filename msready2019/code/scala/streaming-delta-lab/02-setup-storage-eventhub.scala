// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this notebook?
// MAGIC One time setup activities.<br>
// MAGIC 1.  STORAGE: We will mount blob storage containers - staging, raw, curated<br>
// MAGIC 2.  EVENT HUB: We will attach the Azure Event Hub Spark connector to the workspace and cluster<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.0. Set up storage (mount)

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

// MAGIC %run ./01-common

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

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.0. Attach Azure Event Hub connector for Spark to the cluster 

// COMMAND ----------

// MAGIC %md From within your Databricks workspace, create a library as detailed below-

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-aeh-library-attach-1.png)

// COMMAND ----------

// MAGIC %md  Enter the maven coordinates of your Databricks Runtime compatible Azure Event Hub library.  Find the latest [here](https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html).<br>
// MAGIC At the time of authoring the lab it was com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.6

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-aeh-library-attach-2.png)

// COMMAND ----------

// MAGIC %md Select the cluster and click on install

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-aeh-library-attach-3.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-aeh-library-attach-4.png)