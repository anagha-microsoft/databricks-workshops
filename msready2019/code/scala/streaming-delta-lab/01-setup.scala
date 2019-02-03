// Databricks notebook source
// MAGIC %md
// MAGIC ## 1.  Mount blob storage
// MAGIC **Pre-requisite:**<br>
// MAGIC A storage account (gen1) should be available in your pre-provisioned lab environment.<br>
// MAGIC The storage account should have 3 containers - staging, raw and curated, with "private, no anonymous access" configuration.<br>
// MAGIC If they dont exist, go ahead and create them.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.1.  Dependency: Existence of the following storage blob containers
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/0-blob-storage-containers-0.png)

// COMMAND ----------

// MAGIC %md ### 1.2.  Credentials: Storage account
// MAGIC 
// MAGIC **1.2.1. From the portal, navigate to your storage account**<br><br>
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/0-blob-storage-key-1.png)
// MAGIC 
// MAGIC <hr>
// MAGIC <br>
// MAGIC **1.2.2. Copy the storage account name and storage account key; We will use it in the lab**<br><br>
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/0-blob-storage-key-2.png)
// MAGIC     

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.3. Mount containers

// COMMAND ----------

// 0. Storage account credentials
// Replace with your storage account details - hardcoded (secrets out of scope for the lab)
val storageAccountName = dbutils.secrets.get(scope = "ready2019lab", key = "storage-acct-nm")
val storageAccountAccessKey = dbutils.secrets.get(scope = "ready2019lab", key = "storage-acct-key")

// COMMAND ----------

// 1. Create root directory 
dbutils.fs.mkdirs("/mnt/workshop/")

// COMMAND ----------

// Incase already mounted, unmount
dbutils.fs.unmount("/mnt/workshop/staging")
dbutils.fs.unmount("/mnt/workshop/raw")
dbutils.fs.unmount("/mnt/workshop/curated")

// COMMAND ----------

// 2. Mount staging container
var containerName="staging"
dbutils.fs.mount(
  source = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/",
  mountPoint = "/mnt/workshop/" + containerName,
  extraConfigs = Map("fs.azure.account.key." + storageAccountName + ".blob.core.windows.net" -> storageAccountAccessKey))

// COMMAND ----------

// 3. Mount raw container
containerName="raw"
dbutils.fs.mount(
  source = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/",
  mountPoint = "/mnt/workshop/" + containerName,
  extraConfigs = Map("fs.azure.account.key." + storageAccountName + ".blob.core.windows.net" -> storageAccountAccessKey))

// COMMAND ----------

// 4. Mount curated container
containerName="curated"
dbutils.fs.mount(
  source = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/",
  mountPoint = "/mnt/workshop/" + containerName,
  extraConfigs = Map("fs.azure.account.key." + storageAccountName + ".blob.core.windows.net" -> storageAccountAccessKey))

// COMMAND ----------

// 5. List
dbutils.fs.ls("/mnt/workshop/")

// COMMAND ----------

// MAGIC %md 
// MAGIC // 6. Download data **only if you dont have it already** in your staging bob container

// COMMAND ----------

// 6.1. Download data to driver local

// COMMAND ----------

// MAGIC %sh
// MAGIC cd /tmp
// MAGIC rm -rf nyclab-files
// MAGIC mkdir nyclab-files
// MAGIC cd nyclab-files
// MAGIC wget "https://ready2018adbsa.blob.core.windows.net/staging/taxi_zone_lookup.csv"
// MAGIC wget "https://ready2018adbsa.blob.core.windows.net/staging/yellow_tripdata_2017-01.csv"
// MAGIC wget "https://ready2018adbsa.blob.core.windows.net/staging/yellow_tripdata_2017-02.csv"

// COMMAND ----------

display(dbutils.fs.ls("file:/tmp/nyclab-files/"))

// COMMAND ----------

// 6.2.  Upload reference data to DBFS backed by Azure Blob Storage
dbutils.fs.rm("/mnt/workshop/staging/reference-data/",recurse=true)
dbutils.fs.mkdirs("/mnt/workshop/staging/reference-data/")
dbutils.fs.cp("file:/tmp/nyclab-files/taxi_zone_lookup.csv","/mnt/workshop/staging/reference-data/")
display(dbutils.fs.ls("/mnt/workshop/staging/reference-data/"))

// COMMAND ----------

// 6.3.  Upload reference data to DBFS backed by Azure Blob Storage
dbutils.fs.rm("/mnt/workshop/staging/transactions/",recurse=true)
dbutils.fs.mkdirs("/mnt/workshop/staging/transactions/")
dbutils.fs.cp("file:/tmp/nyclab-files/yellow_tripdata_2017-01.csv","/mnt/workshop/staging/transactions/")
dbutils.fs.cp("file:/tmp/nyclab-files/yellow_tripdata_2017-02.csv","/mnt/workshop/staging/transactions/")
display(dbutils.fs.ls("/mnt/workshop/staging/transactions/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.  Azure Event Hub
// MAGIC **Pre-requisite:**<br>
// MAGIC An Azure Event Hub should be available in your pre-provisioned lab environment.<br>
// MAGIC If it does not exist, we will need to create the same.

// COMMAND ----------

// MAGIC %md The following are additional setup tasks for the workshop

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.1.  Create an Azure Event Hub instance in your Azure Event Hub namespace
// MAGIC Name = aeh-nyc | Partitions = 3 | Retention period = 1

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-1.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-2.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-3.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.2. Create a consumer group in your Azure Event Hub instance
// MAGIC Name = nyc-aeh-topic-streaming-cg

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-4.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-5.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-6.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-7.png)

// COMMAND ----------

// MAGIC %md
// MAGIC **(3)  Create a Shared Access Policy for connectivity from Spark**<br> 
// MAGIC Policy Name = RootAccessPolicy | Manage, Listen, Send

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-8.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-9.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-10.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-11.png)

// COMMAND ----------

// MAGIC %md
// MAGIC **(4)  Capture the connection string with primary key for connectivity from Spark**<br> 

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-12.png)