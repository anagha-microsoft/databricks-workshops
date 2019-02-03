// Databricks notebook source
// MAGIC %md
// MAGIC ## 1.  Azure Event Hub
// MAGIC 
// MAGIC #### Pre-requisite:
// MAGIC - An Azure Event Hub should be available in your pre-provisioned lab environment.  If it does not exist, we will need to create the same.<br>
// MAGIC 
// MAGIC #### What's in this notebook:
// MAGIC 1.  We will create an Azure Event Hub instance
// MAGIC 2.  Within the Azure Event Hub instance, we will create a consumer group
// MAGIC 3.  We will create a shared access policy for the Azure Event Hub instance
// MAGIC 4.  We will capture the connection string for the primary key for use in Spark
// MAGIC 5.  We will attach the Spark event hub connector to the cluster

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.1.  Create an Azure Event Hub instance in your Azure Event Hub namespace
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
// MAGIC ### 1.2. Create a consumer group in your Azure Event Hub instance
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
// MAGIC ### 1.3.  Create a Shared Access Policy for connectivity from Spark
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
// MAGIC ### 1.4.  Capture the connection string with primary key for connectivity from Spark

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-create-aeh-12.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.5.  Attach the Event Hub Spark connector library to the cluster

// COMMAND ----------

// MAGIC %md From within your Databricks workspace, create a library as detailed below-

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/msready2019/images/1-aeh-library-attach-1.png)

// COMMAND ----------

// MAGIC %md  Enter the maven coordinates of your Databricks Runtime compatible Azure Event Hub library.  Find the latest [here](https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html)

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