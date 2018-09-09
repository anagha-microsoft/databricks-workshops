// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC 1) Read raw data, parse year out of movie name, parse genre from pipe-separated values<BR>
// MAGIC 2) Persist as Parquet<BR>
// MAGIC 3) Create external unmanaged Hive tables<BR>
// MAGIC 4) Generate statistics for tables  

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
val destDataDir = "/mnt/data/movielens/curatedDir/movies" 

// COMMAND ----------

// MAGIC %md
// MAGIC #3.0. Explore the raw dataset

// COMMAND ----------

// MAGIC %md
// MAGIC #####3.1. Load the dataframe and view records

// COMMAND ----------

val moviesDF = sql("select distinct * from movielens_db.movies_raw")

// COMMAND ----------

moviesDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #####3.2. Profile the data

// COMMAND ----------

//Count
moviesDF.count()

// COMMAND ----------

//Run descriptive statistics
moviesDF.describe().show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from movielens_db.movies_raw where (movie_title is null) or (movie_title='')

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from movielens_db.movies_raw where (movie_genre is null) or (movie_genre='')

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.  Define functions to parse movie data 

// COMMAND ----------

// MAGIC %md
// MAGIC #####4.1. Create function that parses movie name out of the movie title

// COMMAND ----------

def parseMovieName(movieTitle: String): String = {
  movieTitle.substring(0,(movieTitle.length())-6).trim
}

// COMMAND ----------

//Test the function
parseMovieName("Toy Story (1995)")

// COMMAND ----------

// MAGIC %md
// MAGIC #####4.2. Create function that parses movie release year out of the movie title

// COMMAND ----------

def parseMovieYear(movieTitle: String): String = {
  movieTitle.substring(movieTitle.indexOf("(")+1,(movieTitle.length())-1).trim
}

// COMMAND ----------

//Test the function
parseMovieYear("Toy Story (1995)")

// COMMAND ----------

// MAGIC %md
// MAGIC #####4.3. Create function that parses movie genre out of PSV into genre flags

// COMMAND ----------

def parseMovieGenre(genre: String, movieGenrePSV: String): String = {
  if(movieGenrePSV.indexOf(genre) >= 0)
    "Y"
  else
    "N"
}

// COMMAND ----------

//Test the function
parseMovieGenre("action","adventure|animation|action|thriller")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5.  Curate movie dataset
// MAGIC In this section, we will do the following:<BR>
// MAGIC 1. Add column movie name - use function parseMovieName<BR>
// MAGIC 2. Add column movie year - use function parseMovieYear<BR>
// MAGIC 3. Parse the movie_genre pipe delimited value and add the following column flags, one for each genre available across all movies:<BR>
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

//Parse
val curatedMoviesDF = moviesDF.map{x => (x.getInt(0),//movie_id
                                         parseMovieName(x.getString(1)),//movie_name
                                         parseMovieYear(x.getString(1)),//movie_year
                                         parseMovieGenre("action",x.getString(2)),//genre=action
                                         parseMovieGenre("adventure",x.getString(2)),//genre=adventure
                                         parseMovieGenre("animation",x.getString(2)),//genre=animation
                                         parseMovieGenre("children's",x.getString(2)),//genre=children's
                                         parseMovieGenre("comedy",x.getString(2)),//genre=comedy
                                         parseMovieGenre("crime",x.getString(2)),//genre=crime
                                         parseMovieGenre("documentary",x.getString(2)),//genre=documentary
                                         parseMovieGenre("drama",x.getString(2)),//genre=drama
                                         parseMovieGenre("fantasy",x.getString(2)),//genre=fantasy
                                         parseMovieGenre("film-noir",x.getString(2)),//genre=film-noir
                                         parseMovieGenre("horror",x.getString(2)),//genre=horror
                                         parseMovieGenre("musical",x.getString(2)),//genre=musical
                                         parseMovieGenre("mystery",x.getString(2)),//genre=mystery
                                         parseMovieGenre("romance",x.getString(2)),//genre=romance
                                         parseMovieGenre("sci-fi",x.getString(2)),//genre=sci-fi
                                         parseMovieGenre("thriller",x.getString(2)),//genre=thriller
                                         parseMovieGenre("war",x.getString(2)),//genre=war
                                         parseMovieGenre("western",x.getString(2))//genre=western
                                        )
}
.toDF("movie_id","movie_name","movie_year","is_action","is_adventure","is_animation","is_childrens","is_comedy",
     "is_crime","is_documentary","is_drama","is_fantasy","is_filmnoir","is_horror",
     "is_musical","is_mystery","is_romance", 
     "is_scifi","is_thriller","is_war","is_western")
.withColumn("temp_movie_year",col("movie_year").cast(IntegerType)).drop("movie_year").withColumnRenamed("temp_movie_year", "movie_year")

curatedMoviesDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6.  Persist as parquet

// COMMAND ----------

//Delete output from prior executions
dbutils.fs.rm(destDataDir,recurse=true)

// COMMAND ----------

//Persist to parquet format
//The data is less than 1MB, so coalescing
curatedMoviesDF.coalesce(1).write.parquet(destDataDir)

// COMMAND ----------

//Validate
display(dbutils.fs.ls(destDataDir))

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.  Create Hive table

// COMMAND ----------

// MAGIC %sql
// MAGIC use movielens_db;
// MAGIC DROP TABLE IF EXISTS movies_curated;
// MAGIC CREATE TABLE IF NOT EXISTS movies_curated(
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
// MAGIC is_western STRING
// MAGIC )
// MAGIC USING parquet
// MAGIC LOCATION '/mnt/data/movielens/curatedDir/movies/';

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.  Validate table load

// COMMAND ----------

// MAGIC %sql
// MAGIC REFRESH TABLE movielens_db.movies_curated;
// MAGIC SELECT * FROM movielens_db.movies_curated LIMIT 100;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9.  Compute statistics

// COMMAND ----------

//Compute statistics
analyzeTables("movielens_db.movies_curated")