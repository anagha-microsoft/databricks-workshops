// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC In this exercise, we will select the required label and features we have identified for model training and create a dataset in LIBSVM format
// MAGIC 
// MAGIC The label is dep_delay_15 and the features of interest are -<BR>
// MAGIC   origin_airport_id, flight_month, flight_day_of_month,flight_day_of_week, flight_dep_hour, carrier_indx, dest_airport_id, wind_speed,sea_level_pressure,hourly_precip

// COMMAND ----------

import org.apache.spark.mllib.regression.LabeledPoint 
import org.apache.spark.mllib.linalg.Vectors 
import org.apache.spark.mllib.util.MLUtils 
import org.apache.spark.ml.feature.StringIndexer 
import org.apache.spark.ml.feature.VectorAssembler 
import org.apache.spark.mllib.linalg.Vectors 
import org.apache.spark.storage.StorageLevel 

import com.databricks.backend.daemon.dbutils.FileInfo

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 1. Select attributes of interest

// COMMAND ----------

// MAGIC %sql
// MAGIC --Query to capture model training input
// MAGIC --All input needs to be numeric, and specifically double datatype
// MAGIC select distinct dep_delay_15,origin_airport_id,flight_month,flight_day_of_month,flight_day_of_week,flight_dep_hour,carrier_indx,dest_airport_id,wind_speed,sea_level_pressure,hourly_precip from flight_db.materialized_view

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 2.  Create dataframe of model input

// COMMAND ----------

//Create dataframe from query developed above
//Drop rows with any null values
val modelInputDF = sqlContext.sql("""
select distinct 
cast(dep_delay_15 as double),
cast(origin_airport_id as double),
cast(flight_month as double),
cast(flight_day_of_month as double),
cast(flight_day_of_week as double),
cast(flight_dep_hour as double),
cast(carrier_indx as double),
cast(dest_airport_id as double),
cast(wind_speed as double),
cast(sea_level_pressure as double),
cast(hourly_precip as double) 
from flight_db.materialized_view
""").na.drop.cache()

// COMMAND ----------

//Record count
modelInputDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 3.  Create index of features and label 

// COMMAND ----------

//Map feature names to indices 
val featureIndexes = List("origin_airport_id", "flight_month", "flight_day_of_month","flight_day_of_week", "flight_dep_hour", "carrier_indx", "dest_airport_id", "wind_speed","sea_level_pressure","hourly_precip").map(modelInputDF.columns.indexOf(_)) 

// COMMAND ----------

//Map label/target to index 
val labelIndex = modelInputDF.columns.indexOf("dep_delay_15") 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 4.  Create LabeledPoint RDD

// COMMAND ----------

//Convert to LabeledPoint RDD 
val labeledPointRDD = modelInputDF.rdd.map(r => LabeledPoint( 
    // Target value/label
    r.getDouble(labelIndex),  
    // Map feature indices to values 
    Vectors.dense(featureIndexes.map(r.getDouble(_)).toArray))
    )

// COMMAND ----------

labeledPointRDD.take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 5.  Persist

// COMMAND ----------

//Destination diectory
val destinationDirectory="/mnt/data/mlw/scratchDir/model-input"

// COMMAND ----------

//Delete output from prior execution
dbutils.fs.rm(destinationDirectory,recurse=true)

// COMMAND ----------

//Save as LibSVM file 
MLUtils.saveAsLibSVMFile(labeledPointRDD, destinationDirectory) 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### 6.  Review

// COMMAND ----------

//Lets see what we got
display(dbutils.fs.ls(destinationDirectory))

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destinationDirectory + "/").foreach((i: FileInfo) => if (!(i.path contains "part")) dbutils.fs.rm(i.path))

// COMMAND ----------

//Peek into the output
dbutils.fs.head("/mnt/data/mlw/scratchDir/model-input/part-00000")

// COMMAND ----------

