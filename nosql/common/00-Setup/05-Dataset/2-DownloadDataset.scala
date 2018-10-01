// Databricks notebook source
// MAGIC %md
// MAGIC # Download movielens public dataset & persist to DBFS

// COMMAND ----------

// MAGIC %md
// MAGIC ####Goal:
// MAGIC 1.  Download the movielens dataset<BR>
// MAGIC 2.  Persist it into blob storage - to a staging area from where we will process the data
// MAGIC <BR>
// MAGIC <BR>
// MAGIC   
// MAGIC #####Prerequisites:
// MAGIC - 1.  Completion of the mount blob storage exercise

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.  Clean up any data from prior runs of this notebook

// COMMAND ----------

//dbutils.fs.rm("/mnt/data/movielens/stagingDir/", recurse=true)

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.  Download data from the internet to local filesystem on driver machine and upload to blob storage

// COMMAND ----------

// MAGIC %md
// MAGIC ######2.1. Download to local filesystem

// COMMAND ----------

//Clean up any residual data from prior runs
dbutils.fs.rm("file:/tmp/movielens/ml-20m",recurse=true)
dbutils.fs.rm("file:/tmp/movielens/ml-20m.zip")
display(dbutils.fs.ls("file:/tmp/movielens/"))

// COMMAND ----------

//Download
import sys.process._
import scala.sys.process.ProcessLogger

"wget -P /tmp/movielens/ http://files.grouplens.org/datasets/movielens/ml-20m.zip" !!

// COMMAND ----------

// MAGIC %md
// MAGIC ######2.2. Verify

// COMMAND ----------

display(dbutils.fs.ls("file:/tmp/movielens/ml-20m.zip"))

// COMMAND ----------

// MAGIC %md
// MAGIC ######2.3. Unzip

// COMMAND ----------

// MAGIC %sh
// MAGIC cd /tmp/movielens/
// MAGIC unzip ml-20m.zip

// COMMAND ----------

// MAGIC %md
// MAGIC ######2.4. List downloaded files

// COMMAND ----------

display(dbutils.fs.ls("file:/tmp/movielens/ml-20m/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ######2.5. Upload to DBFS - to the staging directory

// COMMAND ----------

val localPath="file:/tmp/movielens/ml-20m/"
val wasbPath="/mnt/data/movielens/stagingDir"

dbutils.fs.cp(localPath + "movies.csv", wasbPath)
dbutils.fs.cp(localPath + "ratings.csv", wasbPath)
dbutils.fs.cp(localPath + "tags.csv", wasbPath)
dbutils.fs.cp(localPath + "links.csv", wasbPath)



// COMMAND ----------

// MAGIC %md
// MAGIC ######2.6. Validate

// COMMAND ----------

display(dbutils.fs.ls(wasbPath))

// COMMAND ----------

// MAGIC %md
// MAGIC ######2.7. Clean up local file system

// COMMAND ----------

dbutils.fs.rm("file:/tmp/movielens/ml-20m",recurse=true)
dbutils.fs.rm("file:/tmp/movielens/ml-20m.zip")

// COMMAND ----------

display(dbutils.fs.ls("file:/tmp/"))