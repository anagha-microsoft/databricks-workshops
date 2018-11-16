// Databricks notebook source
// MAGIC %md
// MAGIC # ADLS gen 2 - primer
// MAGIC Azure Data Lake Storage Gen2 combines the capabilities of two existing storage services: Azure Data Lake Storage Gen1 features, such as file system semantics, file-level security and scale are combined with low-cost, tiered storage, high availability/disaster recovery capabilities, and a large SDK/tooling ecosystem from Azure Blob Storage.<br><br>
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC In this exercise, we will complete the following in batch operations on DBFS backed by ADLS Gen2, with Hierarchical Name Space (HNS):<br>
// MAGIC 1.  Set credentials for access<br>
// MAGIC 2.  Create the root file system<br>
// MAGIC 3.  Create a couple directories, list directories, delete a directory<br>
// MAGIC 4.  Create a dataset and persist to ADLS Gen2, create external table and run queries<br>
// MAGIC 5.  Read data from ADLS Gen2<br>
// MAGIC 
// MAGIC Not supported currently but on the roadmap:<br> 
// MAGIC 1.  Mounting ADLS Gen2 
// MAGIC 2.  SAS tokens 
// MAGIC 
// MAGIC References:<br>
// MAGIC ADLS Gen2 product page:<br>
// MAGIC Databricks ADLS Gen2 integration: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake-gen2.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Credentials setting
// MAGIC Mount points are not supported yet.

// COMMAND ----------

//Replace with your ADLS Gen2 Account name
val adlsgen2acct = "adlsgen2sa"

// COMMAND ----------

//Set the configuration
spark.conf.set("fs.azure.account.key." + adlsgen2acct + ".dfs.core.windows.net", dbutils.secrets.get(scope = "ws-adlsgen2-storage", key = "storage-acct-key")) 
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Create root file system

// COMMAND ----------

dbutils.fs.mkdirs("abfss://dbfs-root@adlsgen2sa.dfs.core.windows.net/")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Create directory

// COMMAND ----------

dbutils.fs.mkdirs("abfss://dbfs-root@adlsgen2sa.dfs.core.windows.net/raw")

// COMMAND ----------

display(dbutils.fs.ls("abfss://dbfs-root@adlsgen2sa.dfs.core.windows.net/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.0. Create dataset

// COMMAND ----------

val booksDF = Seq(
   ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
   ("b00023", "Arthur Conan Doyle", "A sign of four", 1890),
   ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
   ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
   ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901)
).toDF("book_id", "book_author", "book_name", "book_pub_year")

booksDF.printSchema
booksDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5.0. Persist in Delta format to DBFS

// COMMAND ----------

//Destination directory for Delta table
val deltaTableDirectory = "abfss://base-dbfs@adlsgen2sa.dfs.core.windows.net/raw/books"
dbutils.fs.rm(deltaTableDirectory, recurse=true)

//Persist dataframe to delta format without coalescing
booksDF.write.format("delta").save(deltaTableDirectory)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6.0. Create external table

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS books_db_adlsgen2;
// MAGIC 
// MAGIC USE books_db_adlsgen2;
// MAGIC DROP TABLE IF EXISTS books;
// MAGIC CREATE TABLE books
// MAGIC USING DELTA
// MAGIC LOCATION "abfss://base-dbfs@adlsgen2sa.dfs.core.windows.net/raw/books";

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.0. Query your table with SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from books_db_adlsgen2.books;