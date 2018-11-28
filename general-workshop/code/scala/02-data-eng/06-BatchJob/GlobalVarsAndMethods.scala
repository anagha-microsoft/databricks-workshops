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

// JDBC URI
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Properties() object to hold the parameters
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

def generateBatchID(): Int = 
{
  var batchId: Int = 0
  var pushdown_query = "(select count(*) as record_count from BATCH_JOB_HISTORY) table_record_count"
  val df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
  val recordCount = df.first().getInt(0)
  println("Record count=" + recordCount)
  
  if(recordCount == 0)
    batchId=1
  else 
  {
    pushdown_query = "(select max(batch_id) as current_batch_id from BATCH_JOB_HISTORY) current_batch_id"
    val df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
    batchId = df.first().getInt(0) + 1
  }
  batchId
}