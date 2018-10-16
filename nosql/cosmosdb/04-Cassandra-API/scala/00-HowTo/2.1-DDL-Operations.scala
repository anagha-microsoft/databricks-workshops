// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC Basics of how to work with Azure Cosmos DB Cassandra API from Databricks <B>in batch</B>.<BR>
// MAGIC Section 01: Cassandra API connection<BR>
// MAGIC Section 02: DDL operations for keyspace and table<BR>
// MAGIC 
// MAGIC   **Reference:**<br> 
// MAGIC https://github.com/datastax/spark-cassandra-connector/blob/master/doc/1_connecting.md

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.0. Cassandra API connection

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.1. Prerequisites
// MAGIC - Add the 5 Azure Cosmos DB-Cassandra API specific configuration to the cluster configuation described in the ReadMe
// MAGIC - Attach the Databricks runtime compatible Datastax Cassandra connector library to the cluster
// MAGIC - Attach the Azure Cosmos DB Cassandra helper librray to the cluster

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.2. Imports

// COMMAND ----------

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.sql.cassandra._

//Datastax Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

//CosmosDB library for multiple retry
import com.microsoft.azure.cosmosdb.cassandra

//Specify connection factory for Cassandra
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

//Parallelism and throughput configs
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "10")
spark.conf.set("spark.cassandra.output.concurrent.writes", "100")
spark.conf.set("spark.cassandra.concurrent.reads", "512")
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "1000")
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "60000000") //Increase this number as needed
spark.conf.set("spark.cassandra.output.ignoreNulls","true")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.0. Cassandra DDL operations
// MAGIC This section covers-<br>
// MAGIC - (1) DDL for keyspace
// MAGIC - (2) DDL for table

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.1. Keyspace DDL

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.1.1.a. Create keyspace - from cqlsh

// COMMAND ----------

// MAGIC %md
// MAGIC **To connect to ComsosDB Cassandra API:**<br>
// MAGIC <code>export SSL_VERSION=TLSv1_2</code><br>
// MAGIC <code>export SSL_VALIDATE=false</code><br>
// MAGIC <code>cqlsh.py YOUR_ACCOUNT_NAME.cassandra.cosmosdb.azure.com 10350 -u YOUR_ACCOUNT_NAME -p YOUR_ACCOUNT_PASSWORD --ssl</code><br>
// MAGIC 
// MAGIC **To create a keyspace:**<br>
// MAGIC <code>CREATE KEYSPACE IF NOT EXISTS books_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }; </code><br>
// MAGIC   
// MAGIC **To validate keyspace creation:**<br>
// MAGIC <code>DESCRIBE keyspaces;</code><br>
// MAGIC You should see the keyspace you created.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.1.1.b. Create keyspace - from Spark

// COMMAND ----------

//Instantiate cassandra connector
val cdbConnector = CassandraConnector(sc)
// Create keyspace
cdbConnector.withSessionDo(session => session.execute("CREATE KEYSPACE IF NOT EXISTS books_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 } "))

// COMMAND ----------

// MAGIC %md
// MAGIC **Validate in cqlsh**<br>
// MAGIC <code>DESCRIBE keyspaces;</code>

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.1.2. Alter keyspace - from spark

// COMMAND ----------

// MAGIC %md Not supported currently

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.1.3.a. Drop keyspace - from cqlsh

// COMMAND ----------

// MAGIC %md
// MAGIC <code>DROP keyspace books_ks;</code><br>
// MAGIC <code>DESCRIBE keyspaces;</code>

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.1.3.b. Drop keyspace - from Spark

// COMMAND ----------

val cdbConnector = CassandraConnector(sc)
//cdbConnector.withSessionDo(session => session.execute("DROP KEYSPACE books_ks"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.2. Table DDL

// COMMAND ----------

// MAGIC %md
// MAGIC **CosmosDB specific 'must know's:**<br>
// MAGIC &nbsp;&nbsp;-Throughput can be assigned at a table level as part of the create table statement.<br>
// MAGIC &nbsp;&nbsp;-One partition key can store 10 GB of data. <br> 
// MAGIC &nbsp;&nbsp;-One record can be max of 2 MB in size<br>
// MAGIC &nbsp;&nbsp;-One partition key range can store multiple partition keys.<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2.1.a. Create table - from cqlsh

// COMMAND ----------

// MAGIC %md
// MAGIC <code>CREATE TABLE IF NOT EXISTS books_ks.books(<br>
// MAGIC   book_id TEXT PRIMARY KEY,<br>
// MAGIC   book_author TEXT, <br>
// MAGIC   book_name TEXT,<br>
// MAGIC   book_pub_year INT,<br>
// MAGIC   book_price FLOAT) <br>
// MAGIC   WITH cosmosdb_provisioned_throughput=4000;</code>

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2.1.b. Create table from Spark

// COMMAND ----------

val cdbConnector = CassandraConnector(sc)
cdbConnector.withSessionDo(session => session.execute("CREATE TABLE IF NOT EXISTS books_ks.books(book_id TEXT PRIMARY KEY,book_author TEXT, book_name TEXT,book_pub_year INT,book_price FLOAT) WITH cosmosdb_provisioned_throughput=4000 , WITH default_time_to_live=630720000;"))

// COMMAND ----------

// MAGIC %md
// MAGIC **Validate in cqlsh**<br>
// MAGIC <code>USE books_ks;</code><br>
// MAGIC <code>DESCRIBE tables;</code><br>
// MAGIC <code>DESCRIBE books;</code>

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2.2.a. Alter table from Spark

// COMMAND ----------

// MAGIC %md 
// MAGIC (1) Alter table - add/change columns - on the roadmap<BR>
// MAGIC (2) Alter provisioned throughput - supported<BR>
// MAGIC (3) Alter table TTL - supported<BR>

// COMMAND ----------

val cdbConnector = CassandraConnector(sc)
cdbConnector.withSessionDo(session => session.execute("ALTER TABLE books_ks.books WITH cosmosdb_provisioned_throughput=8000,WITH default_time_to_live=0;"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2.2.b. Alter table from cqlsh

// COMMAND ----------

// MAGIC %md
// MAGIC <code>USE books_ks;</code><br>
// MAGIC <code>ALTER TABLE books_ks.books WITH cosmosdb_provisioned_throughput=8000;</code><br>
// MAGIC <code>DESCRIBE books;</code>

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2.3.a. Drop table from Spark

// COMMAND ----------

val cdbConnector = CassandraConnector(sc)
//cdbConnector.withSessionDo(session => session.execute("DROP TABLE IF EXISTS books_ks.books;"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2.3.b. Drop table from cqlsh

// COMMAND ----------

// MAGIC %md
// MAGIC <code>USE books_ks;</code><br>
// MAGIC <code>DROP TABLE IF EXISTS books;</code><br>
// MAGIC <code>DESCRIBE tables;</code><br>