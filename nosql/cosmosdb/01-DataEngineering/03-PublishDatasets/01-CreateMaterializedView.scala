// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC 1) Create a materialized view that combines-<BR>
// MAGIC - All attributes of curated movies<BR>
// MAGIC - Total number of ratings and average rating by movie<BR>
// MAGIC - Total number of tags and listing of tags by movie<BR>
// MAGIC <BR>

// COMMAND ----------

import spark.implicits._
import spark.sql
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.  Execute notebook with common/reusable functions 

// COMMAND ----------

// MAGIC %run "../../00-Setup/04-Common/02-CommonFunctions"

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.  Define destination directory

// COMMAND ----------

//Destination directory
val destDataDir = "/mnt/data/movielens/consumptionDir/materialized-view" 

// COMMAND ----------

// MAGIC %md
// MAGIC ###3.0. Generate aggregates

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 3.0.1. Generate ratings count by movie

// COMMAND ----------

spark.sql("select movie_id, count(*) as ratings_count from movielens_db.ratings_curated group by movie_id").createOrReplaceTempView("movie_ratings_count")
spark.sql("select * from movie_ratings_count limit 2").show

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 3.0.2. Generate average rating by movie

// COMMAND ----------

spark.sql("select movie_id, avg(rating) as average_rating from movielens_db.ratings_curated group by movie_id").createOrReplaceTempView("movie_ratings_average")
spark.sql("select * from movie_ratings_average limit 2").show

// COMMAND ----------

// MAGIC %md
// MAGIC ###4.0. Generate materialized view

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.1. Build the SQL...

// COMMAND ----------

// MAGIC %sql
// MAGIC select mc.*, mra.average_rating, mrc.ratings_count,
// MAGIC tc.tag_count_total,
// MAGIC tc.tag_listing_json
// MAGIC from 
// MAGIC movielens_db.movies_curated mc 
// MAGIC left outer join movie_ratings_count mrc
// MAGIC on (mc.movie_id=mrc.movie_id)
// MAGIC left outer join movie_ratings_average mra
// MAGIC on (mc.movie_id=mra.movie_id)
// MAGIC left outer join movielens_db.tags_curated tc
// MAGIC on (mc.movie_id=tc.movie_id)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.2. Build the dataframe we will use to persist as Parquet

// COMMAND ----------

val matViewDF = spark.sql("select mc.*, mra.average_rating, mrc.ratings_count,tc.tag_count_total,tc.tag_listing_json from movielens_db.movies_curated mc left outer join movie_ratings_count mrc on (mc.movie_id=mrc.movie_id) left outer join movie_ratings_average mra on (mc.movie_id=mra.movie_id) left outer join movielens_db.tags_curated tc on (mc.movie_id=tc.movie_id)").na.fill(0,Seq("tag_count_total")).na.fill("",Seq("tag_listing_json"))

matViewDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ###  5.  Persist as parquet

// COMMAND ----------

//Delete output from prior executions
dbutils.fs.rm(destDataDir,recurse=true)

//Persist to parquet format
matViewDF.coalesce(2).write.parquet(destDataDir)

//Validate
display(dbutils.fs.ls(destDataDir))

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6.  Create Hive table

// COMMAND ----------

// MAGIC %sql
// MAGIC use movielens_db;
// MAGIC DROP TABLE IF EXISTS movies_materialized_view;
// MAGIC CREATE TABLE IF NOT EXISTS movies_materialized_view(
// MAGIC movie_id INT,
// MAGIC movie_name STRING,
// MAGIC movie_year INT,
// MAGIC is_action STRING,
// MAGIC is_adventure STRING,
// MAGIC is_animation STRING,
// MAGIC is_childrens STRING,
// MAGIC is_comedy STRING,
// MAGIC is_crime STRING,
// MAGIC is_documentary STRING,
// MAGIC is_drama STRING,
// MAGIC is_fantasy STRING,
// MAGIC is_filmnoir STRING,
// MAGIC is_horror STRING,
// MAGIC is_musical STRING,
// MAGIC is_mystery STRING,
// MAGIC is_romance STRING,
// MAGIC is_scifi STRING,
// MAGIC is_thriller STRING,
// MAGIC is_war STRING,
// MAGIC is_western STRING,
// MAGIC average_rating DOUBLE,
// MAGIC ratings_count LONG,
// MAGIC tag_count_total LONG,
// MAGIC tag_listing_json STRING
// MAGIC )
// MAGIC USING parquet
// MAGIC LOCATION '/mnt/data/movielens/consumptionDir/materialized-view/';

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.  Validate table load

// COMMAND ----------

// MAGIC %sql
// MAGIC REFRESH TABLE movielens_db.movies_materialized_view;
// MAGIC SELECT * FROM movielens_db.movies_materialized_view LIMIT 100;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.  Compute statistics

// COMMAND ----------

//Compute statistics
analyzeTables("movielens_db.movies_materialized_view")

// COMMAND ----------

