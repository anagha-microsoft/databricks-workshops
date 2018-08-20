// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC We will run various reports and visualize                          

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.  Trip count by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   taxi_type,
// MAGIC   count(*) as trip_count
// MAGIC from 
// MAGIC   <userid>_taxi_db.taxi_trips_mat_view
// MAGIC group by taxi_type

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select 
// MAGIC   taxi_type,
// MAGIC   count(*) as trip_count
// MAGIC from 
// MAGIC   <userid>_taxi_db.taxi_trips_mat_view
// MAGIC group by taxi_type

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.  Revenue including tips by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.  Revenue share by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   taxi_type, sum(total_amount) revenue
// MAGIC from 
// MAGIC   <userid>_taxi_db.taxi_trips_mat_view
// MAGIC group by taxi_type

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.  Trip revenue trend between January and May

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5.  Trip count trend by month, by taxi type, for 2016

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6.  Average trip distance by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.  Average trip amount by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.  Trips with no tip, by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9.  Trips with no charge, by taxi type

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### 10.  Trips by payment type

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### 11.  Trip trend by pickup hour for yellow taxi in 2016

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### 12.  Top 3 yellow taxi pickup-dropoff zones for 2016

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO

// COMMAND ----------

