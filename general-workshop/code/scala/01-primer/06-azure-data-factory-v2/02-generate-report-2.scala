// Databricks notebook source
//dbutils.widgets.removeAll

// COMMAND ----------

//Create widget for batch id
dbutils.widgets.text("new_batch_id","")

// COMMAND ----------

//Capture batch ID
val batchID: Int = dbutils.widgets.get("new_batch_id").toInt

// COMMAND ----------

// MAGIC %run ./00-common

// COMMAND ----------

insertBatchMetadata(batchID,2,"Execute report 2","Started")

// COMMAND ----------

//Generate report 2
val reportDF = spark.sql("SELECT case_year,primary_type as case_type, count(*) AS crime_count FROM crimes_db.chicago_crimes_curated GROUP BY case_year,primary_type")

// COMMAND ----------

import org.apache.spark.sql.SaveMode

//Persist report dataset to destination RDBMS
reportDF.coalesce(1).write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "CHICAGO_CRIMES_COUNT_BY_YEAR", connectionProperties)

// COMMAND ----------

//Mark as completed
//In your code - enhance to ensure data did get persisted
insertBatchMetadata(batchID,2,"Execute report 2","Completed")