// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC In this exercise, we will score a dataset of raw data needing predictions, using the model created in the previous module, and persist predictions to an RDBMS

// COMMAND ----------

import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.RandomForestClassificationModel


// COMMAND ----------

//Directory where model is stored
val modelDirectory="/mnt/data/mlw/modelDir/flight-delay/randomForest/manuallyTuned"

//Source directory - data to score
val sourceFile = "/mnt/data/mlw/stagingDir/batch-scoring/BatchScoringDataset.csv"

//Destination directory - predictions
val destinationDirectory = "/mnt/data/mlw/consumptionDir/batch-predictions"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1. Read source needing scoring

// COMMAND ----------

//Read the data needing scoring into a dataframe, drop rows with nulls
val batchScoringInputDF = sqlContext.read
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .csv(sourceFile)
    .cache()

// COMMAND ----------

//Materialize
batchScoringInputDF.count

// COMMAND ----------

//Check count of distinct rows to identify dupes
batchScoringInputDF.distinct.count

// COMMAND ----------

//Check count of distinct rows without nulls
batchScoringInputDF.na.drop().distinct.count

// COMMAND ----------

//Peek
batchScoringInputDF.show(3)

// COMMAND ----------

//View statistics
batchScoringInputDF.describe().show()

// COMMAND ----------

//View statistics of a specific column
batchScoringInputDF.select("Carrier").distinct().show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 2. Create a temp view of the scoring input

// COMMAND ----------

//Register temp table
batchScoringInputDF.distinct.createOrReplaceTempView("prediction_input")

// COMMAND ----------

// MAGIC %sql
// MAGIC desc prediction_input

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 3. Profile the scoring input

// COMMAND ----------

// MAGIC %sql
// MAGIC --select count(*) from prediction_input
// MAGIC --select count(*) from prediction_input where WindSpeed is null 
// MAGIC --select count(*) from prediction_input where WindSpeed=''
// MAGIC --select count(*) from prediction_input where cast(WindSpeed as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --select count(*) from prediction_input where SeaLevelPressure is null 
// MAGIC --select count(*) from prediction_input where SeaLevelPressure=''
// MAGIC --select count(*) from prediction_input where cast(WindSpeed as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC --select count(*) from prediction_input where HourlyPrecip is null 
// MAGIC --select count(*) from prediction_input where HourlyPrecip=''
// MAGIC --select count(*) from prediction_input where cast(HourlyPrecip as decimal(5,3)) is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select distinct * from
// MAGIC (
// MAGIC   select
// MAGIC   a.OriginAirportCode,
// MAGIC   a.Month,
// MAGIC   a.DayofMonth,
// MAGIC   a.CRSDepHour,
// MAGIC   a.DayOfWeek,
// MAGIC   a.Carrier,
// MAGIC   a.DestAirportCode,
// MAGIC   a.WindSpeed,
// MAGIC   a.SeaLevelPressure,
// MAGIC   a.HourlyPrecip,
// MAGIC   amo.airport_lat as origin_airport_latitude,
// MAGIC   amo.airport_long as origin_airport_longitude,
// MAGIC   amd.airport_lat as dest_airport_latitude,
// MAGIC   amd.airport_long as dest_airport_longitude,
// MAGIC   cast(amo.airport_id as double) as bsi_origin_airport_id,
// MAGIC   cast(a.month as double) as bsi_flight_month,
// MAGIC   cast(a.DayofMonth as double) as bsi_flight_day_of_month,
// MAGIC   cast(a.DayOfWeek as double) as bsi_flight_day_of_week,
// MAGIC   cast(round(a.CRSDepHour/100,0) as double) as bsi_flight_dep_hour,
// MAGIC   cast(cm.carrier_indx as double) as bsi_carrier_indx,
// MAGIC   cast(amd.airport_id as double) as bsi_dest_airport_id,
// MAGIC   cast(a.WindSpeed as double) as bsi_wind_speed,
// MAGIC   cast(a.SeaLevelPressure as double) as bsi_sea_level_pressure,
// MAGIC   cast(a.HourlyPrecip as double) as bsi_hourly_precip
// MAGIC   from 
// MAGIC   prediction_input a
// MAGIC   left outer join flight_db.airport_master amo 
// MAGIC   on (a.OriginAirportCode=amo.airport_cd)
// MAGIC   left outer join flight_db.airport_master amd
// MAGIC   on (a.DestAirportCode=amd.airport_cd)
// MAGIC   left outer join
// MAGIC   flight_db.carrier_master cm
// MAGIC   on (a.Carrier=cm.carrier_cd)
// MAGIC ) x

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 4. Create materialized temp view of the scoring input dataset and other master, reference data - drop nulls and dupes

// COMMAND ----------

//Join with master data to prepare a dataset that has input as well as attributes needed by the model
val batchScoringMatViewDF = sqlContext.sql("""
select distinct * from
(
  select
  a.OriginAirportCode,
  a.Month,
  a.DayofMonth,
  a.CRSDepHour,
  a.DayOfWeek,
  a.Carrier,
  a.DestAirportCode,
  a.WindSpeed,
  a.SeaLevelPressure,
  a.HourlyPrecip,
  amo.airport_lat as origin_airport_latitude,
  amo.airport_long as origin_airport_longitude,
  amd.airport_lat as dest_airport_latitude,
  amd.airport_long as dest_airport_longitude,
  cast(amo.airport_id as double) as bsi_origin_airport_id,
  cast(a.month as double) as bsi_flight_month,
  cast(a.DayofMonth as double) as bsi_flight_day_of_month,
  cast(a.DayOfWeek as double) as bsi_flight_day_of_week,
  cast(round(a.CRSDepHour/100,0) as double) as bsi_flight_dep_hour,
  cast(cm.carrier_indx as double) as bsi_carrier_indx,
  cast(amd.airport_id as double) as bsi_dest_airport_id,
  cast(a.WindSpeed as double) as bsi_wind_speed,
  cast(a.SeaLevelPressure as double) as bsi_sea_level_pressure,
  cast(a.HourlyPrecip as double) as bsi_hourly_precip
  from 
  prediction_input a
  left outer join flight_db.airport_master amo 
  on (a.OriginAirportCode=amo.airport_cd)
  left outer join flight_db.airport_master amd
  on (a.DestAirportCode=amd.airport_cd)
  left outer join
  flight_db.carrier_master cm
  on (a.Carrier=cm.carrier_cd)
) x
""").na.drop.cache()

//Register temp table
batchScoringMatViewDF.distinct.createOrReplaceTempView("pred_input_mat_vw")


// COMMAND ----------

//Stats
batchScoringMatViewDF.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 5. Create dataframe for scoring

// COMMAND ----------

val modelInputDF = sqlContext.sql("""
select distinct 
bsi_origin_airport_id as origin_airport_id,
bsi_flight_month as flight_month,
bsi_flight_day_of_month as flight_day_of_month,
bsi_flight_day_of_week as flight_day_of_week,
bsi_flight_dep_hour as flight_dep_hour,
bsi_carrier_indx as carrier_indx,
bsi_dest_airport_id as dest_airport_id,
bsi_wind_speed as wind_speed,
bsi_sea_level_pressure as sea_level_pressure,
bsi_hourly_precip as hourly_precip
from pred_input_mat_vw
""").na.drop.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 6. Load model

// COMMAND ----------

//Load model
val model = RandomForestClassificationModel.load(modelDirectory)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 7. Create dataframe of feature vector

// COMMAND ----------


val assembler = new VectorAssembler().setInputCols(Array("origin_airport_id","flight_month","flight_day_of_month","flight_day_of_week","flight_dep_hour","carrier_indx","dest_airport_id","wind_speed","sea_level_pressure","hourly_precip")).setOutputCol("features")

val featureVectorForBatchScoringDF = assembler.transform(modelInputDF)

// COMMAND ----------

featureVectorForBatchScoringDF.show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 8. Run predictions

// COMMAND ----------

val predictionsDF = model.transform(featureVectorForBatchScoringDF).select("origin_airport_id","flight_month","flight_day_of_month","flight_day_of_week","flight_dep_hour","carrier_indx","dest_airport_id","wind_speed","sea_level_pressure","hourly_precip", "probability", "prediction")

// COMMAND ----------

predictionsDF.show(3)

// COMMAND ----------

//Reverting back to the original input and appending the probability and prediction
predictionsDF.createOrReplaceTempView("batch_predictions")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 9. Join predictions back with scoring inout data

// COMMAND ----------

val modelOutputDF = sqlContext.sql("""
select 
pi.OriginAirportCode,
pi.Month,
pi.DayofMonth,
pi.CRSDepHour,
pi.DayOfWeek,
pi.Carrier,
pi.DestAirportCode,
pi.WindSpeed,
pi.SeaLevelPressure,
pi.HourlyPrecip,
case cast(po.prediction AS INT) when 0 then "False" else "True" end as Delayed,
pi.origin_airport_latitude,
pi.origin_airport_longitude,
pi.dest_airport_latitude,
pi.dest_airport_longitude
from 
batch_predictions po
left outer join
pred_input_mat_vw pi
on
(
po.origin_airport_id = pi.bsi_origin_airport_id AND
po.flight_month = pi.bsi_flight_month AND
po.flight_day_of_month = pi.bsi_flight_day_of_month AND
po.flight_day_of_week = pi.bsi_flight_day_of_week AND
po.flight_dep_hour = pi.bsi_flight_dep_hour AND
po.carrier_indx = pi.bsi_carrier_indx AND
po.dest_airport_id = pi.bsi_dest_airport_id AND
po.wind_speed = pi.bsi_wind_speed AND
po.sea_level_pressure = pi.bsi_sea_level_pressure AND
po.hourly_precip = pi.bsi_hourly_precip 
)
""").cache()

// COMMAND ----------

//Materialize
modelOutputDF.count()
modelOutputDF.printSchema

// COMMAND ----------

//Quick review
modelOutputDF.show(5)

// COMMAND ----------

//Partitions
println(modelOutputDF.rdd.partitions.length)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 10. Persist predictions to RDBMS
// MAGIC 
// MAGIC ##### TODO: Secure credentials

// COMMAND ----------

//https://docs.databricks.com/spark/latest/data-sources/sql-databases.html

//1) Add the databricks IP range to sql server firewall
//2) Create table in Azure SQL database
/*
DROP TABLE IF EXISTS PREDICTIONS_COMPLETED;
CREATE TABLE PREDICTIONS_COMPLETED(
OriginAirportCode VARCHAR(3),
Month INT,
DayofMonth INT,
CRSDepHour INT,
DayOfWeek INT,
Carrier VARCHAR(3),
DestAirportCode  VARCHAR(3),
WindSpeed DECIMAL(5,3),
SeaLevelPressure DECIMAL(5,3),
HourlyPrecip DECIMAL(5,3),
Delayed VARCHAR(5),
OriginAirportLatitude DECIMAL(9,6),
OriginAirportLongitude DECIMAL(9,6),
DestinationAirportLatitude DECIMAL(9,6),
DestinationAirportLongitude DECIMAL(9,6)
);
*/

// COMMAND ----------

//Database credentials
// Define the credentials and parameters
val hostname = "bhoomidbsrvr.database.windows.net"
val dbName = "bhoomidb"
val jdbcPort = 1433
val user = "akhanolk"
val passwd = "A1rawat#123"

val jdbcUrl = (s"jdbc:sqlserver://${hostname}:${jdbcPort};database=${dbName};user=${user};password=${passwd}")

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val connectionProperties = new java.util.Properties()
connectionProperties.setProperty("Driver",driverClass)

// COMMAND ----------

//Persist predictions
modelOutputDF.coalesce(25).write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "PREDICTIONS_COMPLETED", connectionProperties)