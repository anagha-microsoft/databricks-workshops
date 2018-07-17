// Databricks notebook source
val scratchDirectory = "/mnt/data/mlw/scratchDir/flight-delay/randomForest/transformedScoringDataset"

// COMMAND ----------

// MAGIC %sql
// MAGIC --This table is in Azure SQL Database
// MAGIC CREATE TABLE IF NOT EXISTS flight_db.predictions_needed
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://bhoomidbsrvr.database.windows.net:1433;database=bhoomidb',
// MAGIC   dbtable 'predictions_needed',
// MAGIC   user 'akhanolk',
// MAGIC   password 'A1rawat#123'
// MAGIC )

// COMMAND ----------

//Read the data needing scoring into a dataframe, drop rows with nulls
val batchScoringInputDF = sqlContext.sql("""
select * from flight_db.predictions_needed
""").na.drop().distinct.cache()

//Materialize
batchScoringInputDF.count

// COMMAND ----------

//Register temp table
batchScoringInputDF.createOrReplaceTempView("prediction_input")

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

// COMMAND ----------

//Save as table
batchScoringMatViewDF.write.mode(SaveMode.Overwrite).option("path", scratchDirectory).saveAsTable("flight_db.pred_input_mat_vw")