// Databricks notebook source
// MAGIC %run ./00-common

// COMMAND ----------

//Capture batch ID
val batchID: Int = generateBatchID()

// COMMAND ----------

insertBatchMetadata(batchID,1,"Execute report 1","Started")

// COMMAND ----------

//Generate report 1
val reportDF = spark.sql("SELECT primary_type as case_type, count(*) AS crime_count FROM crimes_db.chicago_crimes_curated GROUP BY primary_type")

// COMMAND ----------

import org.apache.spark.sql.SaveMode

//Persist report dataset to destination RDBMS
reportDF.coalesce(1).write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "CHICAGO_CRIMES_COUNT", connectionProperties)

// COMMAND ----------

//Mark as completed
//In your code - enhance to ensure data did get persisted
insertBatchMetadata(batchID,1,"Execute report 1","Completed")

// COMMAND ----------

dbutils.notebook.exit("Pass")