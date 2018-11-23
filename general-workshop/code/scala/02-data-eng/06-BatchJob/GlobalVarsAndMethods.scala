// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_reports_db;
// MAGIC USE taxi_reports_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS BATCH_JOB_HISTORY;
// MAGIC CREATE TABLE BATCH_JOB_HISTORY
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://bhoomidbsrvr.database.windows.net:1433;database=bhoomidb;',
// MAGIC   dbtable 'BATCH_JOB_HISTORY',
// MAGIC   user 'akhanolk',
// MAGIC   password "A1rawat#123"
// MAGIC );

// COMMAND ----------

//Database credentials & details - for use with Spark scala for writing
//1) JDBC driver class & connection properties
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val connectionProperties = new java.util.Properties()
connectionProperties.setProperty("Driver",driverClass)
//2) JDBC URL
val jdbcUrl = (s"jdbc:sqlserver://bhoomidbsrvr.database.windows.net:1433;database=bhoomidb;user=akhanolk;password=A1rawat#123")

// COMMAND ----------

def generateBatchID(): Int = 
{
  var batchId: Int = 0
  val recordCount = sql("select count(*) from taxi_reports_db.BATCH_JOB_HISTORY").first().getLong(0)
  println("Record count=" + recordCount)

  if(recordCount == 0)
    batchId=1
  else 
    batchId= sql("select max(batch_id) from taxi_reports_db.BATCH_JOB_HISTORY").first().getInt(0) + 1
 
  batchId
}