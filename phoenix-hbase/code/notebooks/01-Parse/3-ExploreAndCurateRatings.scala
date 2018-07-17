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
import java.util.Date
import java.text.SimpleDateFormat

def epochToDateTime(epochTime: Long, formatType: String): String = {
  var parsedDateString=""
  
  if(formatType.equals("DateTime"))
  {
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    parsedDateString = dateFormat.format(epochTime * 1000L)
  } 
  else if(formatType.equals("Date"))
  {
    val dateFormat= new SimpleDateFormat("yyyy-MM-dd")
    parsedDateString = dateFormat.format(epochTime * 1000L)
  }
  else
  {
    val dateFormat= new SimpleDateFormat("yyyy")
    parsedDateString = dateFormat.format(epochTime * 1000L)
  }
  parsedDateString
}    

val ratingsDF = sc.textFile("/mnt/data/movielens/stagingDir/ratings.dat").map{ line =>
  val fields = line.split("::")
  (epochToDateTime(fields(3).toLong,"DateTime"),epochToDateTime(fields(3).toLong,"Date"), epochToDateTime(fields(3).toLong,"Year"), fields(0).toInt, fields(1).toInt, fields(2).toDouble)
}.toDF("rating_ts","rating_date","rating_year","user_id","movie_id","rating")
ratingsDF.show()

// COMMAND ----------

//Remove any prior existence of files
val destinationDirRoot = "/mnt/data/movielens/rawDir/ratings/"
dbutils.fs.rm(destinationDirRoot,recurse=true)

//Persist to DBFS
ratingsDF.coalesce(1).write.csv(destinationDirRoot)

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
// MAGIC DROP TABLE IF EXISTS ratings;
// MAGIC CREATE EXTERNAL TABLE ratings (
// MAGIC   rating_ts STRING,
// MAGIC   rating_date STRING,
// MAGIC   rating_year INT,
// MAGIC   user_id INT,
// MAGIC   movie_id INT,
// MAGIC   rating DOUBLE)
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY ','
// MAGIC LOCATION '/mnt/data/movielens/rawDir/ratings';
// MAGIC   
// MAGIC ANALYZE TABLE users COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from movielens_db.ratings;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 5. Descriptive stats

// COMMAND ----------

val df = sql("""select * from movielens_db.ratings""").cache()

// COMMAND ----------

df.count

// COMMAND ----------

df.describe().show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from movielens_db.ratings where movie_id=1