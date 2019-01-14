// Databricks notebook source
// MAGIC %md
// MAGIC ## Run a report and integrate with SQL RDBMS

// COMMAND ----------

// MAGIC %md ##### 1.  Define RDBMS credentials

// COMMAND ----------

val jdbcUsername = dbutils.secrets.get(scope = "adfv2ws-sql-db", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "adfv2ws-sql-db", key = "password")
val jdbcHostname = dbutils.secrets.get(scope = "adfv2ws-sql-db", key = "server")
val jdbcDatabase = dbutils.secrets.get(scope = "adfv2ws-sql-db", key = "database")
val jdbcPort = 1433

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

// MAGIC %md ##### 2.  Capture counts by time buckets

// COMMAND ----------

spark.sql("select time_bucket, count(*) as trip_count from nyc_db.taxi_trips_curated group by time_bucket").createOrReplaceGlobalTempView("report1")

// COMMAND ----------

//%sql
//select * from global_temp.report1

// COMMAND ----------

// MAGIC %md ##### 3.  Persist the report to Azure SQL Database 

// COMMAND ----------

import org.apache.spark.sql.SaveMode

spark.table("global_temp.report1")
     .write
     .mode(SaveMode.Overwrite) // <--- Overwrite the existing table
     .jdbc(jdbcUrl, "report1", connectionProperties)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4. Exit notebook

// COMMAND ----------

dbutils.notebook.exit("Pass")