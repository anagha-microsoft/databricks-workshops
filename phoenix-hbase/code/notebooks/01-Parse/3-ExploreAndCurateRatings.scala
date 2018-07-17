// Databricks notebook source
// MAGIC %md
// MAGIC # Explore and curate movies
// MAGIC Hbase workshop | MovieLens dataset

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 1. Run common functions module

// COMMAND ----------

// MAGIC %run "../00-Setup/02-CommonFunctions"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 2. Explore raw file in staging directory

// COMMAND ----------

// MAGIC %fs head /mnt/data/movielens/stagingDir/ratings.dat

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 3. Parse and persist to DBFS, to rawDir

// COMMAND ----------

import spark.sqlContext.implicits._

val ratingsDF = sc.textFile("/mnt/data/movielens/stagingDir/ratings.dat").map{ line =>
  val fields = line.split("::")
  (fields(3).toLong, fields(0).toInt, fields(1).toInt, fields(2).toDouble)
}.toDF("rating_epoch_ts","user_id","movie_id","rating")
ratingsDF.show()

// COMMAND ----------

//Remove any prior existence of files
val destinationDirRoot = "/mnt/data/movielens/rawDir/movies/"
dbutils.fs.rm(destinationDirRoot,recurse=true)

// COMMAND ----------

//Persist to DBFS
moviesDF.coalesce(1).write.csv(destinationDirRoot)

//Delete flag files
recursivelyDeleteSparkJobFlagFiles(destinationDirRoot)

// COMMAND ----------

//List files
display(dbutils.fs.ls(destinationDirRoot))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 4. Create external table

// COMMAND ----------

// MAGIC %sql
// MAGIC use movielens_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS movies;
// MAGIC CREATE EXTERNAL TABLE movies (
// MAGIC   id INT,
// MAGIC   name STRING,
// MAGIC   year INT,
// MAGIC   genre_list STRING)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY ','
// MAGIC LOCATION '/mnt/data/movielens/rawDir/movies';
// MAGIC   
// MAGIC ANALYZE TABLE users COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from movielens_db.movies;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 5. Descriptive stats

// COMMAND ----------

val df = sql("""select * from movielens_db.movies""").cache()

// COMMAND ----------

df.count

// COMMAND ----------

df.describe().show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from movielens_db.movies where id=1