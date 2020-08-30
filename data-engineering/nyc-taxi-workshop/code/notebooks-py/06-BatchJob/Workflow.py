# Databricks notebook source
# MAGIC %run ./GlobalVarsAndMethods

# COMMAND ----------

batchID = generateBatchID()

# COMMAND ----------

sql("insert into <userid>_taxi_reports_db.BATCH_JOB_HISTORY VALUES({}, 1, 'Execute report 1','Started', CURRENT_TIMESTAMP)".format(batchID))
executionStatusReport1 = dbutils.notebook.run("Report-1", 60)

if(executionStatusReport1 == "Pass"):
  sql("insert into <userid>_taxi_reports_db.BATCH_JOB_HISTORY VALUES({}, 1, 'Execute report 1','Completed', CURRENT_TIMESTAMP)".format(batchID))

# COMMAND ----------

executionStatusReport2 = "-"
if(executionStatusReport1 == "Pass"):
  sql("insert into <userid>_taxi_reports_db.BATCH_JOB_HISTORY VALUES({}, 2, 'Execute report 2','Started', CURRENT_TIMESTAMP)".format(batchID))
  executionStatusReport2 = dbutils.notebook.run("Report-2", 60)
  sql("insert into <userid>_taxi_reports_db.BATCH_JOB_HISTORY VALUES({}, 2, 'Execute report 2','Completed', CURRENT_TIMESTAMP)".format(batchID))


# COMMAND ----------

dbutils.notebook.exit(executionStatusReport2)