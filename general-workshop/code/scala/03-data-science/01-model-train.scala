// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC ## Predict trip duration model - prep->transform->train->test->persist
// MAGIC The goal of this tutorial is -
// MAGIC 1.  Adding any missing transformations required for the training exercise
// MAGIC 2.  Using correlation to determine feature importance

// COMMAND ----------

// MAGIC %md #### 1.0. Pre-process the data
// MAGIC 
// MAGIC We added a number of temporal attributes as part of the curation process in the data engineering lab.  One thing we missed is "duration" of trip.  We will build a model to predict duration of a trip, given a date and pickup - dropoff location combination.<br>
// MAGIC 
// MAGIC In order to do this - we need to augment the materialized view with duration - leveraging Spark SQL's datetime functions.  We will drop outliers and bad data. Duration will be our "label" and we will perform correlation analysis to identify features of importance.

// COMMAND ----------

//1.  Function to calculate duration
import org.apache.spark.sql.functions._
val calcDurationFunc: org.apache.spark.sql.Column = col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC //2. Build out the SQL to filter, transform - off of the materialized view from the data engineering workshop

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   pickup_datetime,
// MAGIC   dropoff_datetime,
// MAGIC   cast(dropoff_datetime as long) as dropoff_timelong, 
// MAGIC   trip_distance,
// MAGIC   pickup_location_id,
// MAGIC   dropoff_location_id,
// MAGIC   rate_code_id,
// MAGIC   store_and_fwd_flag, 
// MAGIC   payment_type,
// MAGIC   fare_amount,
// MAGIC   extra,
// MAGIC   pickup_hour,
// MAGIC   pickup_day as pickup_day_of_week,
// MAGIC   dayofmonth(pickup_datetime) as pickup_day_of_month,
// MAGIC   pickup_minute,
// MAGIC   weekofyear(pickup_datetime) as pickup_week_of_year,
// MAGIC   pickup_month
// MAGIC from 
// MAGIC   taxi_db.taxi_trips_mat_view 
// MAGIC where 
// MAGIC   trip_distance > 0 and (fare_amount > 0 and fare_amount < 5000) and taxi_type='yellow';

// COMMAND ----------

//3.  Create a dataframe off of the SQL developed
val df = spark.sql("select pickup_datetime,dropoff_datetime,cast(dropoff_datetime as long) as dropoff_timelong,trip_distance,pickup_location_id,dropoff_location_id,rate_code_id,store_and_fwd_flag, payment_type,fare_amount,extra,pickup_hour,pickup_day as pickup_day_of_week,dayofmonth(pickup_datetime) as pickup_day_of_month,pickup_minute,weekofyear(pickup_datetime) as pickup_week_of_year,pickup_month from taxi_db.taxi_trips_mat_view where trip_distance > 0 and (fare_amount > 0 and fare_amount < 5000) and taxi_type='yellow'")

// COMMAND ----------

//4.  Augment with trip duration
//This is where the data scientist asks the data engineering team to add derived attributes of interest to the materialized view
val df2 = df
  .withColumn( "duration_minutes", calcDurationFunc / 60D).alias("duration_minutes")
  .drop("pickup_datetime","dropoff_datetime")
  .createOrReplaceTempView("filtered_trips")
  //.withColumn("duration_seconds", calcDurationFunc).alias("duration_seconds")
  //.withColumn( "duration_hours",  calcDurationFunc / 3600D).alias("duration_hours")
  //.withColumn( "duration_days", calcDurationFunc / (24D * 3600D)).alias("duration_days")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from filtered_trips

// COMMAND ----------

//5.  Lets drop data with 0 duration or less if at all
val df3 = spark.sql("select * from filtered_trips where duration_minutes > 0")

// COMMAND ----------

display(df3)

// COMMAND ----------

// MAGIC %md #### 2. Feature importance 
// MAGIC Correlation is used to test relationships between quantitative variables or categorical variables. In other words, it’s a measure of how things are related. Correlations are useful because if you can find out what relationship variables have, you can make predictions about future behavior.
// MAGIC 
// MAGIC Spark ML provides us with library functions to perform correlation and other basic statistic operations.

// COMMAND ----------

// MAGIC %md Correlation coefficients range between -1 and +1. The closer the value is to +1, the higher the positive correlation is, i.e the values are more related to each other. The closer the correlation coefficient is to -1, the more the negative correlation is. It can be mentioned here that having a negative correlation does not mean the values are less related, just that one variable increases as the other decreases.
// MAGIC 
// MAGIC For example, a correlation of -0.8 is better than a correlation of 0.4 and the values are more related in the first case than in the second.

// COMMAND ----------

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val ls= new ListBuffer[(Double, String)]()
println("\nCorrelation Analysis :")
   	for ( field <-  df3.schema) {
		if ( ! field.dataType.equals(StringType)) {
          var x= df3.stat.corr("duration_minutes", field.name)
          var tuple : (Double, String) = (x,field.name)
          ls+=tuple
		}
	}
//Let us sort the correlation values in descending order to see the fields in the order of their relation with the duration field
// Sorting our correlation values in descending order to see the most related fields first
val lsMap= ls.toMap
val sortedMap= ListMap(lsMap.toSeq.sortWith(_._1 > _._1):_*)
sortedMap.collect{
  case (value, field_name) => println("Correlation between duration_minutes and " + field_name + " = " + value)
}

// COMMAND ----------

// MAGIC %md 
// MAGIC From our analysis, we can observe that the `duration_minutes` column has the highest correlation with `trip_distance`, followed by `rate_code_id`, `extra` and `fare_amount` column, which makes sense as the time taken for a trip to complete is directly related to the distance travelled.

// COMMAND ----------

// MAGIC %md #### 3. Feature Transformation#

// COMMAND ----------

// MAGIC %md 
// MAGIC Since we are going to try algorithms like Linear Regression, *we need all the categorical variables in the dataset as numeric variables*. <br>
// MAGIC There are 2 options:
// MAGIC 
// MAGIC **1.  Category Indexing**
// MAGIC 
// MAGIC This is basically assigning a numeric value to each category from {0, 1, 2, ...numCategories-1}. This introduces an implicit ordering among your categories, and is more suitable for ordinal variables (eg: Poor: 0, Average: 1, Good: 2)
// MAGIC 
// MAGIC **2.  One-Hot Encoding**
// MAGIC 
// MAGIC This converts categories into binary vectors with at most one nonzero value (eg: (Blue: [1, 0]), (Green: [0, 1]), (Red: [0, 0]))

// COMMAND ----------

// MAGIC %md ##### Indexing
// MAGIC 
// MAGIC StringIndexer encodes a string column of labels to a column of label indices. The indices are in [0, numLabels), ordered by label frequencies, so the most frequent label gets index 0. The unseen labels will be put at index numLabels if user chooses to keep them. If the input column is numeric, we cast it to string and index the string values. When downstream pipeline components such as Estimator or Transformer make use of this string-indexed label, you must set the input column of the component to this string-indexed column name. In many cases, you can set the input column with setInputCol.
// MAGIC 
// MAGIC The dataset has categorical data in the pickup_location_id, dropoff_location_id, store_and_fwd_flag and rate_code_id columns. We will use a string indexer to convert these to numeric labels

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val indexerPickup: org.apache.spark.ml.feature.StringIndexerModel = new StringIndexer()
                   .setInputCol("pickup_location_id")
                   .setOutputCol("pickup_location_id_indxd")
                   .fit(df3)
val indexerDropoff: org.apache.spark.ml.feature.StringIndexerModel = new StringIndexer()
                    .setInputCol("dropoff_location_id")
                    .setOutputCol("dropoff_location_id_indxd")
                    .fit(df3)
val indexerStoreFlag: org.apache.spark.ml.feature.StringIndexerModel =new StringIndexer()
                    .setInputCol("store_and_fwd_flag")
                    .setOutputCol("store_and_fwd_flag_indxd")
                    .fit(df3)
val indexerRatecode: org.apache.spark.ml.feature.StringIndexerModel = new StringIndexer()
                    .setInputCol("rate_code_id")
                    .setOutputCol("rate_code_id.indxd")
                    .fit(df3)

val indexedDF: org.apache.spark.sql.DataFrame =indexerPickup.transform(
                   indexerDropoff.transform(
                     indexerStoreFlag.transform(
                       indexerRatecode.transform(df3))))

// COMMAND ----------

display(indexedDF)

// COMMAND ----------

