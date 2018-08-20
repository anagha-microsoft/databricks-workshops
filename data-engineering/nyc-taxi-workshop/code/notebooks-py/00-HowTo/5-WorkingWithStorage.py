# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC 
# MAGIC 1.  101 on working with databricks file system - Azure blob storage

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0. Azure Blob Storage
# MAGIC 
# MAGIC In the previous section, we mounted blob storage, we will use the scratchDir directory to learn with Databricks file system backed by Azure Blob Storage.
# MAGIC The following are covered-
# MAGIC 1.  Create a directory
# MAGIC 2.  Download a file to local file system
# MAGIC 3.  Save the local file to blob storage
# MAGIC 4.  List blob storage directory contents
# MAGIC 5.  Delete the local file
# MAGIC 6.  Copy a file
# MAGIC 7.  Delete a file
# MAGIC 8.  Delete a directory

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.1. Create a directory if it does not exist

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /mnt/<userid>/data/nyctaxi/scratchDir/<userid>

# COMMAND ----------

# MAGIC %md
# MAGIC We got this errror because the directory already exists

# COMMAND ----------

display(dbutils.fs.ls("/mnt/<userid>/data/nyctaxi/scratchDir/<userid>"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.2. Download a file to the local file system

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -P /tmp "https://nyctaxidew.blob.core.windows.net/nyctaxi-demo/If-By-Kipling.txt"

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp | grep If

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /tmp/If-By-Kipling.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.3. Load the file to Azure Blob Storage

# COMMAND ----------

dbutils.fs.cp("file:/tmp/If-By-Kipling.txt","/mnt/<userid>/data/nyctaxi/scratchDir/<userid>/testDir/")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.4. List contents of Azure Blob Storage

# COMMAND ----------

display(dbutils.fs.ls("/mnt/<userid>/data/nyctaxi/scratchDir/<userid>/testDir"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.5. Delete local file

# COMMAND ----------

dbutils.fs.rm("file:/tmp/If-By-Kipling.txt")

# COMMAND ----------

dbutils.fs.ls("file:/tmp/If-By-Kipling.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.6. Copy a file

# COMMAND ----------

# Will not work as /mlw doesn't exist, but example is worth keeping


# dbutils.fs.cp("/mnt/data/mlw/scratchDir/testDir/If-By-Kipling.txt","/mnt/data/nyctaxi/scratchDir/testDir/If-By-Kipling-2.txt")
# dbutils.fs.cp("/mnt/data/mlw/scratchDir/testDir/If-By-Kipling.txt","/mnt/data/nyctaxi/scratchDir/testDir/If-By-Kipling-3.txt")
# dbutils.fs.cp("/mnt/data/mlw/scratchDir/testDir/If-By-Kipling.txt","/mnt/data/nyctaxi/scratchDir/testDir/If-By-Kipling-4.txt")
# dbutils.fs.cp("/mnt/data/mlw/scratchDir/testDir/If-By-Kipling.txt","/mnt/data/nyctaxi/scratchDir/testDir/If-By-Kipling-5.txt")
# dbutils.fs.cp("/mnt/data/mlw/scratchDir/testDir/If-By-Kipling.txt","/mnt/data/nyctaxi/scratchDir/testDir/If-By-Kipling-6.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.7. Delete a file

# COMMAND ----------

dbutils.fs.rm("/mnt/<userid>/data/nyctaxi/scratchDir/<userid>/testDir/If-By-Kipling-6.txt")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/<userid>/nyctaxi/scratchDir/<userid>/testDir/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0.8. Delete a directory recursively

# COMMAND ----------

dbutils.fs.rm("/mnt/<userid>/data/nyctaxi/scratchDir/<userid>/testDir/",recurse=true)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/<userid>/data/nyctaxi/scratchDir/<userid>/"))