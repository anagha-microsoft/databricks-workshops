// Databricks notebook source
//Execute the load
val executionStatusNotebook1 = dbutils.notebook.run("01-load", 60)

// COMMAND ----------

var executionStatusNotebook2 = "-"

//If load completes, execute the curation notebook
if(executionStatusNotebook1 == "Pass")
{
  executionStatusReport2 = dbutils.notebook.run("02-curate", 60)
}

// COMMAND ----------

//Return the status of the execution of the notebook
dbutils.notebook.exit(executionStatusNotebook2)