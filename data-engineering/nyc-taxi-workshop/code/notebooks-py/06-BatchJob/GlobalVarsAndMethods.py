# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS <userid>_taxi_reports_db;
# MAGIC USE <userid>_taxi_reports_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS BATCH_JOB_HISTORY;
# MAGIC CREATE TABLE BATCH_JOB_HISTORY
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://<yourDatabaseServer>.database.windows.net:1433;database=<yourDatabase>;',
# MAGIC   dbtable 'BATCH_JOB_HISTORY',
# MAGIC   user '<yourUserID>',
# MAGIC   password "yourPassword"
# MAGIC );

# COMMAND ----------

#1) JDBC driver class & connection properties
connectionProperties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

#JDBC URL
jdbcUrl = ("jdbc:sqlserver://<yourDatabaseServer>.database.windows.net:1433;database=<yourDatabase>;user=<yourUserID>;password=yourPassword")

# COMMAND ----------

def generateBatchID():
  batchId = 0
  recordCount = sql("select count(*) from <userid>_taxi_reports_db.BATCH_JOB_HISTORY").first()[0]
  print("Record count={}".format(recordCount))

  if(recordCount == 0):
    batchId=1
  else:
    batchId= sql("select max(batch_id) from <userid>_taxi_reports_db.BATCH_JOB_HISTORY").first()[0] + 1
 
  return batchId
