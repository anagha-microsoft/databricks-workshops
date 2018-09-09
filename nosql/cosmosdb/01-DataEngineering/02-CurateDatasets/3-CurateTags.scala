// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC Curate user tags associated with movies-<BR>
// MAGIC - 1) Create tags dataset that includes movie, and tag with counts
// MAGIC     - Read raw data, 
// MAGIC     - augment with movie data, 
// MAGIC     - add datatime column based off of epoch value
// MAGIC     - create JSON to include tag and tag count
// MAGIC     - drop user<BR>
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
val destDataDir = "/mnt/data/movielens/curatedDir/tags" 

// COMMAND ----------

// MAGIC %md
// MAGIC ###3.0. Explore the raw dataset

// COMMAND ----------

// MAGIC %md
// MAGIC #####3.0.1. Load the dataframe and view records

// COMMAND ----------

val tagsBaseDF = sql("select distinct * from movielens_db.tags_raw")

// COMMAND ----------

tagsBaseDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #####3.0.2. Profile the data

// COMMAND ----------

//Count
tagsBaseDF.count()

// COMMAND ----------

//Run descriptive statistics
tagsBaseDF.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC %md
// MAGIC ### 4.  Curate the base ratings dataset

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.1. Create temporary table for count by tag
// MAGIC Table: movie_by_tag_count

// COMMAND ----------

//Movie - tag count by tag
spark.sql("select distinct movie_id, tag, count(*) as tag_count from movielens_db.tags_raw group by movie_id, tag").createOrReplaceTempView("movie_by_tag_count")
spark.sql("select * from movie_by_tag_count where movie_id = 2716").show

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.2. Create temporary table movie and tag count
// MAGIC Table: movie_tag_count

// COMMAND ----------

//Movie - total tag count
spark.sql("select distinct movie_id, count(*) as tag_count_total from movielens_db.tags_raw group by movie_id").createOrReplaceTempView("movie_tag_count")
spark.sql("select * from movie_tag_count where movie_id=2716").show

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.3. Create a temporary table that lists tags and associated counts by movie
// MAGIC Table: movie_tag_listing

// COMMAND ----------

//Step 1: Create dataset with movieID and json of count by tag
//e.g. 69481, {"tag_counts":[{"tag":"suicide bomber","tag_count":1},{"tag":"guns","tag_count":1},{"tag":"realistic","tag_count":16}...]}
val tagCountDF=spark.sql("select * from movie_by_tag_count")
tagCountDF.printSchema
val formattedTagCountDF = tagCountDF.withColumn("tag_listing", to_json(struct($"tag",$"tag_count")))
                                               .drop("tag")
                                              .drop("tag_count")

formattedTagCountDF
              .groupBy("movie_id")
              .agg(concat_ws(",", collect_list("tag_listing")) as "temp_tag_listing")
              .withColumn("tag_listing", concat(lit("{"),lit("\""), lit("tag_counts"), lit("\""),lit(":["),col("temp_tag_listing"),lit("]}")))
              .drop("temp_tag_listing").createOrReplaceTempView("movie_tag_listing")

// COMMAND ----------

// MAGIC %sql select * from movie_tag_listing

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.4. Join movie with relevant temporary tables
// MAGIC Table: movie_tag_listing

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct mc.*,mtc.tag_count_total, mtl.tag_listing as tag_listing_json
// MAGIC from 
// MAGIC movielens_db.movies_curated mc 
// MAGIC left outer join movie_tag_count mtc
// MAGIC on (mc.movie_id=mtc.movie_id)
// MAGIC left outer join movie_tag_listing mtl
// MAGIC on (mc.movie_id=mtl.movie_id)
// MAGIC where mc.movie_id=2716

// COMMAND ----------

//Final dataset
val curatedTagsDF = spark.sql("select distinct mc.*,mtc.tag_count_total, mtl.tag_listing as tag_listing_json from movielens_db.movies_curated mc left outer join movie_tag_count mtc on (mc.movie_id=mtc.movie_id) left outer join movie_tag_listing mtl on (mc.movie_id=mtl.movie_id)")

// COMMAND ----------

curatedTagsDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ###  5.  Persist as parquet

// COMMAND ----------

//Delete output from prior executions
dbutils.fs.rm(destDataDir,recurse=true)

// COMMAND ----------

//Persist to parquet format
curatedTagsDF.coalesce(1).write.parquet(destDataDir)

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
// MAGIC DROP TABLE IF EXISTS tags_curated;
// MAGIC CREATE TABLE IF NOT EXISTS tags_curated(
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
// MAGIC tag_count_total LONG, 
// MAGIC tag_listing_json STRING
// MAGIC )
// MAGIC USING parquet
// MAGIC LOCATION '/mnt/data/movielens/curatedDir/tags/';

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.  Validate table load

// COMMAND ----------

// MAGIC %sql
// MAGIC REFRESH TABLE movielens_db.tags_curated;
// MAGIC SELECT * FROM movielens_db.tags_curated LIMIT 100;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.  Compute statistics

// COMMAND ----------

//Compute statistics
analyzeTables("movielens_db.tags_curated")