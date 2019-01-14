// Databricks notebook source
// MAGIC %md
// MAGIC ### 1.  Extract from staging and load to raw information zone

// COMMAND ----------

//Execute the load
val executionStatusNotebook1 = dbutils.notebook.run("../../01-ELT/01-load", 120)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.  Transform the raw dataset

// COMMAND ----------

var executionStatusNotebook2 = "-"

//If load completes, execute the curation notebook
if(executionStatusNotebook1 == "Pass")
{
  executionStatusNotebook2 = dbutils.notebook.run("../../01-ELT/02-curate", 120)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.  Run report and pipe to RDBMS

// COMMAND ----------

var executionStatusNotebook3 = "-"

//If load completes, execute the curation notebook
if(executionStatusNotebook2 == "Pass")
{
  executionStatusNotebook3 = dbutils.notebook.run("../../02-Reporting/run-report", 120)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.  Exit with execution status

// COMMAND ----------

//Return the status of the execution of the notebook
dbutils.notebook.exit(executionStatusNotebook3)