// Databricks notebook source
// MAGIC %md
// MAGIC # HDInsight-Kafka: Querying a streaming Delta table- primer
// MAGIC 
// MAGIC ### What's in this exercise?
// MAGIC We will query a Delta table and review operations of importance WRT Delta in the streaming context
// MAGIC 
// MAGIC **Dependency**: <br>
// MAGIC Completion of stream-producer-primer lab.<br>
// MAGIC The producer should be running or have completed.<br>
// MAGIC 
// MAGIC **Docs**: <br>
// MAGIC Databricks Delta: https://docs.databricks.com/delta/index.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Create table on streaming output
// MAGIC This can be run only after some data has accumulated as we are inferring schema from data present.  An alternate way is to use a full DDL that details schema.

// COMMAND ----------

// MAGIC %sql
// MAGIC --Took the author 2 minutes for 1.5 GB of data/6.7 M rows
// MAGIC CREATE DATABASE IF NOT EXISTS crimes_db;
// MAGIC 
// MAGIC USE crimes_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS chicago_crimes_delta_kafka_streaming;
// MAGIC CREATE TABLE chicago_crimes_delta_kafka_streaming
// MAGIC USING DELTA
// MAGIC LOCATION '/mnt/data/workshop/curatedDir/chicago-crimes-data-delta-kafka-streaming';

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Run queries

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from chicago_crimes_delta_kafka_streaming;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from chicago_crimes_delta_kafka_streaming;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Optimization

// COMMAND ----------

//1) Lets look at part file count
display(dbutils.fs.ls("/mnt/data/workshop/curatedDir/chicago-crimes-data-delta-kafka-streaming"))

// COMMAND ----------

// MAGIC %sql DESCRIBE DETAIL crimes_db.chicago_crimes_delta_kafka_streaming;
// MAGIC --Lets run DESCRIBE DETAIL 
// MAGIC --Review numFiles 

// COMMAND ----------

// MAGIC %sql
// MAGIC --Now, lets run optimize
// MAGIC OPTIMIZE chicago_crimes_delta_kafka_streaming;

// COMMAND ----------

// MAGIC %sql DESCRIBE DETAIL chicago_crimes_delta_kafka_streaming;
// MAGIC --Notice the number of files now 

// COMMAND ----------

//Lets look at the part file count
//Guess why?
display(dbutils.fs.ls("/mnt/data/workshop/curatedDir/chicago-crimes-data-delta-kafka-streaming"))