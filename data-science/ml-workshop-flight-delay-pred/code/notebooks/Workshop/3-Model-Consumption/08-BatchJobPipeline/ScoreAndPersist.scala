// Databricks notebook source
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.RandomForestClassificationModel

// COMMAND ----------

//Database related

//1) JDBC driver class & connection properties
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val connectionProperties = new java.util.Properties()
connectionProperties.setProperty("Driver",driverClass)

//2) JDBC URL
val jdbcUrl = (s"jdbc:sqlserver://bhoomidbsrvr.database.windows.net:1433;database=bhoomidb;user=akhanolk;password=A1rawat#123")

// COMMAND ----------

//Directory where model is stored
val modelDirectory = "/mnt/data/mlw/modelDir/flight-delay/randomForest/manuallyTuned"

// COMMAND ----------

//Load model
val model = RandomForestClassificationModel.load(modelDirectory)

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
from flight_db.pred_input_mat_vw
""").na.drop.cache()

// COMMAND ----------

//Create feature vector DF
val assembler = new VectorAssembler().setInputCols(Array("origin_airport_id","flight_month","flight_day_of_month","flight_day_of_week","flight_dep_hour","carrier_indx","dest_airport_id","wind_speed","sea_level_pressure","hourly_precip")).setOutputCol("features")

val featureVectorForBatchScoringDF = assembler.transform(modelInputDF)

// COMMAND ----------

//Run predictions
val predictionsDF = model.transform(featureVectorForBatchScoringDF).select("origin_airport_id","flight_month","flight_day_of_month","flight_day_of_week","flight_dep_hour","carrier_indx","dest_airport_id","wind_speed","sea_level_pressure","hourly_precip", "probability", "prediction")

// COMMAND ----------

//Reverting back to the original input and appending the probability and prediction
predictionsDF.createOrReplaceTempView("batch_predictions")

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
flight_db.pred_input_mat_vw pi
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
""")

// COMMAND ----------

//Persist predictions to destination RDBMS
modelOutputDF.coalesce(25).write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "PREDICTIONS_COMPLETED", connectionProperties)