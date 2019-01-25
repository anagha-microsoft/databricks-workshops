# Databricks notebook source
//JDBC connectivity related
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")
val jdbcPort = 1433
val jdbcDatabase = "gws_sql_db"

//Replace with your server name
val jdbcHostname = "gws-server.database.windows.net"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)