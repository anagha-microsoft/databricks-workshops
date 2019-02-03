// Databricks notebook source
// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC 
// MAGIC USE taxi_db;
// MAGIC DROP TABLE IF EXISTS trips;
// MAGIC CREATE TABLE trips
// MAGIC USING DELTA
// MAGIC LOCATION "/mnt/workshop/curated/transactions/";

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from trips limit 10;