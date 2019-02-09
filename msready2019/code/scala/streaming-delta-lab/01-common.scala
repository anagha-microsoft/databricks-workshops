// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this notebook?
// MAGIC In this notebook, we define variables for storage account credentials and event hub credentials and walk through capturing the same for the service, from the portal and assigning to variables created.  This notebook will be called from within any notebooks that need storage and/or event hub access.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Storage credentials configuration

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.0.1.  Dependency: Existence of the following storage blob containers
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/0-blob-storage-containers-0.png)

// COMMAND ----------

// MAGIC %md #### 1.0.2.  Capture credentials of your storage account for mounting
// MAGIC 
// MAGIC **1.0.2.1. From the portal, navigate to your storage account**<br><br>
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/0-blob-storage-key-1.png)
// MAGIC 
// MAGIC <hr>
// MAGIC <br>
// MAGIC **1.0.2.2. Copy the storage account name and storage account key; We will use it in command 5**<br><br>
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/0-blob-storage-key-2.png)
// MAGIC     

// COMMAND ----------

// MAGIC %md **1.0.3.  Assign credentials of your storage account for mounting, into variables**

// COMMAND ----------

// 1.  Storage account conf
// Replace the storage account name and key with yours, hard-coded, for simplicity
val storageAccountName = dbutils.secrets.get(scope = "ready2019lab", key = "storage-acct-nm")
val storageAccountAccessKey = dbutils.secrets.get(scope = "ready2019lab", key = "storage-acct-key")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Event hub credentials configuration

// COMMAND ----------

// MAGIC %md **2.0.1. Create a shared access policy (one time) for your event hub instance** (NOT namespace)<br>
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

// MAGIC %md **2.0.2. Capture the connection string-primary key from the policy created**<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-12.png)

// COMMAND ----------

// MAGIC %md **2.0.3. Assign the primary key connection string to a variable**<br>

// COMMAND ----------

val aehConexionCreds = dbutils.secrets.get(scope = "ready2019lab", key = "aeh-conexion-string")