// Databricks notebook source
// MAGIC %run ./GlobalVarsAndMethods

// COMMAND ----------

val batchID: Long = generateBatchID()

// COMMAND ----------

sql("insert into taxi_reports_db.BATCH_JOB_HISTORY VALUES(" + batchID + ", 1, 'Execute report 1','Started', CURRENT_TIMESTAMP)")
val executionStatusReport1 = dbutils.notebook.run("Report-1", 60)

if(executionStatusReport1 == "Pass")
  sql("insert into taxi_reports_db.BATCH_JOB_HISTORY VALUES(" + batchID + ", 1, 'Execute report 1','Completed', CURRENT_TIMESTAMP)")

// COMMAND ----------

var executionStatusReport2 = "-"
if(executionStatusReport1 == "Pass")
{
  sql("insert into taxi_reports_db.BATCH_JOB_HISTORY VALUES(" + batchID + ", 2, 'Execute report 2','Started', CURRENT_TIMESTAMP)")
  executionStatusReport2 = dbutils.notebook.run("Report-2", 60)
  sql("insert into taxi_reports_db.BATCH_JOB_HISTORY VALUES(" + batchID + ", 2, 'Execute report 2','Completed', CURRENT_TIMESTAMP)")
  
}

// COMMAND ----------

dbutils.notebook.exit(executionStatusReport2)