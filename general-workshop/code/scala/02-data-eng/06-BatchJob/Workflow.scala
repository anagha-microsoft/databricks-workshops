// Databricks notebook source
// MAGIC %run ./GlobalVarsAndMethods

// COMMAND ----------

val batchID: Long = generateBatchID()

// COMMAND ----------

insertBatchMetadata(1,1,"Execute report 1","Started")
val executionStatusReport1 = dbutils.notebook.run("Report-1", 60)

if(executionStatusReport1 == "Pass")
  insertBatchMetadata(1,1,"Execute report 1","Completed")

// COMMAND ----------

var executionStatusReport2 = "-"
if(executionStatusReport1 == "Pass")
{
  insertBatchMetadata(1,2,"Execute report 2","Started")
  executionStatusReport2 = dbutils.notebook.run("Report-2", 60)
  insertBatchMetadata(1,2,"Execute report 2","Completed")
  
}

// COMMAND ----------

dbutils.notebook.exit(executionStatusReport2)