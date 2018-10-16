// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC Basics of how to work with Azure Cosmos DB-Cassandra API from Databricks <B>in batch</B>.<BR>
// MAGIC Section 05: Upsert operation (crUd)<BR>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 5.0.1. Upsert/Update operation (CR*U*D)

// COMMAND ----------

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.sql.cassandra._

//datastax Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

//CosmosDB library for multiple retry
import com.microsoft.azure.cosmosdb.cassandra

// Specify connection factory for Cassandra
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

// Parallelism and throughput configs
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "10")
spark.conf.set("spark.cassandra.output.concurrent.writes", "100")
spark.conf.set("spark.cassandra.concurrent.reads", "512")
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "1000")
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "60000000") //Increase this number as needed

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.0.1. Dataframe API

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 5.0.1.1. Upsert

// COMMAND ----------

// (1) Update: Changing author name to include prefix of "Sir", and (2) Insert: adding a new book
val booksUpsertDF = Seq(
                         ("b00001", "Sir Arthur Conan Doyle", "A study in scarlet", 1887),
                         ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890),
                         ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
                         ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
                         ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901),
                         ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905)
                        ).toDF("book_id", "book_author", "book_name", "book_pub_year")
booksUpsertDF.show()

// COMMAND ----------

// Upsert is no different from create
booksUpsertDF.write
  .mode("append")
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC **TODO**: Add modify TTL

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 5.0.1.2. Update

// COMMAND ----------

val booksUpdateDF = Seq(
                         ("b00001", 5.99),
                         ("b00023", 7.50),
                         ("b01001", 12.25),
                         ("b00501", 12.00),
                         ("b00300", 18.00),
                         ("b09999", 23.99)
                        ).toDF("book_id", "book_price")
booksUpdateDF.show()

booksUpdateDF.write
  .mode("append")
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.0.2. RDD API
// MAGIC No different from create operation

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.0.3. Update using cql in Spark

// COMMAND ----------

//Runs on driver, use wisely
val cdbConnector = CassandraConnector(sc)
cdbConnector.withSessionDo(session => session.execute("update books_ks.books set book_price=99.33 where book_id ='b00300';"))