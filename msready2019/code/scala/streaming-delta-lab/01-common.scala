// Databricks notebook source
// 1.  Storage account conf
// Replace the storage account name and key with yours, hard-coded (secrets not in scope for workshop)
val storageAccountName = dbutils.secrets.get(scope = "ready2019lab", key = "storage-acct-nm")
val storageAccountAccessKey = dbutils.secrets.get(scope = "ready2019lab", key = "storage-acct-key")

// COMMAND ----------

// 2.  Event Hub conf
// Note: You will have this information only after finishing module 02*
val aehConexionCreds = dbutils.secrets.get(scope = "ready2019lab", key = "aeh-conexion-string")