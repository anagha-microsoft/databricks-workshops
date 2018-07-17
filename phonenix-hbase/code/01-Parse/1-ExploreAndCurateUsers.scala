// Databricks notebook source
// MAGIC %md
// MAGIC # Explore and curate users
// MAGIC Hbase workshop | MovieLens dataset

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 1. Explore & copy to raw directory
// MAGIC Users is traight-forward to parse; So, we will copy to rawDir and then create a table on top of it

// COMMAND ----------

// MAGIC %fs head dbfs:/mnt/data/movielens/stagingDir/users.dat

// COMMAND ----------

val destinationDirectoryRoot = "/mnt/data/movielens/rawDir/users/"
dbutils.fs.cp("/mnt/data/movielens/stagingDir/users.dat",destinationDirectoryRoot)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 2. Regex parse into Users table, compute statistics

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC use movielens_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS users;
// MAGIC CREATE EXTERNAL TABLE users (
// MAGIC   user_id INT,
// MAGIC   gender STRING,
// MAGIC   age INT,
// MAGIC   occupation_id INT,
// MAGIC   zipcode STRING
// MAGIC )
// MAGIC ROW FORMAT
// MAGIC   SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
// MAGIC WITH SERDEPROPERTIES (
// MAGIC   'input.regex' = '([^:]+)::([^:]+)::([^:]+)::([^:]+)::([^:]+)'
// MAGIC )
// MAGIC LOCATION 
// MAGIC   '/mnt/data/movielens/rawDir/users*.dat';
// MAGIC   
// MAGIC ANALYZE TABLE users COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %sql
// MAGIC describe extended users;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 3. Explore

// COMMAND ----------

// MAGIC %sql select * from movielens_db.users;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 4. Review descriptive statistics

// COMMAND ----------

val df = sql("""select * from movielens_db.users""").cache()

// COMMAND ----------

df.count()

// COMMAND ----------

df.describe().show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct gender from movielens_db.users;

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct age from movielens_db.users;

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct occupation_id from movielens_db.users;