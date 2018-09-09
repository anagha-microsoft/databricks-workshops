// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC Steps to read 4MB tags dataset (tags from users by movie) from staging directory, optimize storage format of raw datasets (parquet) and persist, create Hive external tables for queryability, and finally compute statistics to improve performance<BR>
// MAGIC   
// MAGIC <B>Steps:</B><BR>
// MAGIC 1) Load data in staging directory to the raw data directory and persist to Parquet format<BR> 
// MAGIC 2) Create external unmanaged Hive tables<BR>
// MAGIC 3) Compute statistics for tables<BR>

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import com.databricks.backend.daemon.dbutils.FileInfo

// COMMAND ----------

//Define source and destination directories
val srcDataFile = "/mnt/data/movielens/stagingDir/tags.csv" //Source data (in staging directory)
val destDataDir = "/mnt/data/movielens/rawDir/tags" //Directory for raw consumable data in parquet

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.  Execute notebook with common/reusable functions 

// COMMAND ----------

// MAGIC %run "../../00-Setup/04-Common/02-CommonFunctions"

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. List dataset

// COMMAND ----------

display(dbutils.fs.ls(srcDataFile))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Explore tags dataset

// COMMAND ----------

dbutils.fs.head(srcDataFile)

// COMMAND ----------

// MAGIC %md
// MAGIC ###4. Define schema for tags dataset

// COMMAND ----------

val tagsSchema = StructType(Array(
   StructField("user_id", IntegerType, true),
    StructField("movie_id", IntegerType, true),
    StructField("tag", StringType, true),
    StructField("tag_timestamp", LongType, true)))

// COMMAND ----------

// MAGIC %md
// MAGIC ###5. Create external table definition

// COMMAND ----------

// MAGIC %sql
// MAGIC use movielens_db;
// MAGIC DROP TABLE IF EXISTS tags_raw;
// MAGIC CREATE TABLE IF NOT EXISTS tags_raw(
// MAGIC user_id INT,
// MAGIC movie_id INT,
// MAGIC tag STRING,
// MAGIC tag_timestamp BIGINT)
// MAGIC USING parquet
// MAGIC LOCATION '/mnt/data/movielens/rawDir/tags/';

// COMMAND ----------

// MAGIC %md
// MAGIC ###6. Read csv and persist to parquet

// COMMAND ----------

//Read source data
val tagsDF = sqlContext.read.format("csv")
                      .option("header", "true")
                      .schema(tagsSchema)
                      .option("delimiter",",")
                      .load(srcDataFile).cache()

// COMMAND ----------

//Explore
tagsDF.show

// COMMAND ----------

//TODO: Add a ense rank query for top N - just for kicks

// COMMAND ----------

//Delete output from prior executions
dbutils.fs.rm(destDataDir,recurse=true)

// COMMAND ----------

//Persist to parquet format, calling function to calculate number of partition files
//The data is less than 1MB, so coalescing
tagsDF.coalesce(1).write.parquet(destDataDir)

// COMMAND ----------

//Validate
display(dbutils.fs.ls(destDataDir))

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC ###7. Validate table load

// COMMAND ----------

// MAGIC %sql
// MAGIC REFRESH TABLE movielens_db.tags_raw;
// MAGIC SELECT * FROM movielens_db.tags_raw limit 100;

// COMMAND ----------

// MAGIC %md
// MAGIC ###8. Compute statistics on Hive table

// COMMAND ----------

//Compute statistics
analyzeTables("movielens_db.tags_raw")