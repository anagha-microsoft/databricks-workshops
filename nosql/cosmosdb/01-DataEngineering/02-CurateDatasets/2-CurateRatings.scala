// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC Curate userID based movie ratings-<BR>
// MAGIC - 1) Read raw data, augment with movie data, add datatime column based off of epoch value<BR>
// MAGIC - 2) Persist as Parquet<BR>
// MAGIC - 3) Create external unmanaged tables<BR>
// MAGIC - 4) Generate statistics for table 

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
val destDataDir = "/mnt/data/movielens/curatedDir/ratings" 

// COMMAND ----------

// MAGIC %md
// MAGIC ###3.0. Explore the raw dataset

// COMMAND ----------

// MAGIC %md
// MAGIC #####3.0.1. Load the dataframe and view records

// COMMAND ----------

val ratingsDF = sql("select distinct * from movielens_db.ratings_raw")

// COMMAND ----------

ratingsDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #####3.0.2. Profile the data

// COMMAND ----------

//Count
ratingsDF.count()

// COMMAND ----------

//Run descriptive statistics
ratingsDF.describe().show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from movielens_db.ratings_raw where (movie_id is null) or (rating is null)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.  Curate the base ratings dataset
// MAGIC In this section, we will do the following:<BR>
// MAGIC 1. Add column movie name<BR>
// MAGIC 2. Add column movie year<BR>
// MAGIC 3. Add movie_genre columns<BR>
// MAGIC genre_Action<BR>
// MAGIC genre_Adventure<BR>
// MAGIC genre_Animation<BR>
// MAGIC genre_Children's<BR>
// MAGIC genre_Comedy<BR>
// MAGIC genre_Crime<BR>
// MAGIC genre_Documentary<BR>
// MAGIC genre_Drama<BR>
// MAGIC genre_Fantasy<BR>
// MAGIC genre_FilmNoir<BR>
// MAGIC genre_Horror<BR>
// MAGIC genre_Musical<BR>
// MAGIC genre_Mystery<BR>
// MAGIC genre_Romance<BR>
// MAGIC genre_SciFi<BR>
// MAGIC genre_Thriller<BR>
// MAGIC genre_War<BR>
// MAGIC genre_Western<BR>

// COMMAND ----------

//Join with movies
val curatedRatingsBaseDF = spark.sql("SELECT DISTINCT R.user_id, R.movie_id,R.rating,R.rating_timestamp,M.movie_name, M.movie_year, M.is_action, M.is_adventure, M.is_animation, M.is_childrens, M.is_comedy, M.is_crime, M.is_documentary, M.is_drama, M.is_fantasy, M.is_filmnoir, M.is_horror, M.is_musical, M.is_mystery, M.is_romance, M.is_scifi, M.is_thriller, M.is_war, M.is_western from movielens_db.ratings_raw R left outer join movielens_db.movies_curated M ON R.movie_id=M.movie_id")

//Parse epoch 
val curatedRatingsDF = curatedRatingsBaseDF.withColumn("ratings_date", from_unixtime(col("rating_timestamp").divide(1000)))

//Quick look
curatedRatingsDF.show

// COMMAND ----------

//Schema
curatedRatingsDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ###  5.  Persist as parquet

// COMMAND ----------

//Delete output from prior executions
dbutils.fs.rm(destDataDir,recurse=true)

// COMMAND ----------

//Persist to parquet format
curatedRatingsDF.coalesce(2).write.parquet(destDataDir)

// COMMAND ----------

//Validate
display(dbutils.fs.ls(destDataDir))

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6.  Create Hive table

// COMMAND ----------

// MAGIC %sql
// MAGIC use movielens_db;
// MAGIC DROP TABLE IF EXISTS ratings_curated;
// MAGIC CREATE TABLE IF NOT EXISTS ratings_curated(
// MAGIC user_id INT,
// MAGIC movie_id INT,
// MAGIC rating FLOAT,
// MAGIC rating_epoch TIMESTAMP,
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
// MAGIC ratings_date STRING
// MAGIC )
// MAGIC USING parquet
// MAGIC LOCATION '/mnt/data/movielens/curatedDir/ratings/';

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.  Validate table load

// COMMAND ----------

// MAGIC %sql
// MAGIC REFRESH TABLE movielens_db.ratings_curated;
// MAGIC SELECT * FROM movielens_db.ratings_curated LIMIT 100;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.  Compute statistics

// COMMAND ----------

//Compute statistics
analyzeTables("movielens_db.ratings_curated")