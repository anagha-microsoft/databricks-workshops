// Databricks notebook source
//Database credentials & details - for use with Spark scala for writing

// Secrets
val jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")

// JDBC driver class & connection properties
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbcHostname = "gws-server.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "gws_sql_db"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)

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