// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC Basics of how to work with Azure Cosmos DB - Cassandra API from Databricks <B>in batch</B>.<BR>
// MAGIC Section 04: Read operation (cRud)<BR>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4.0. Read operation (C*R*UD)

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
// MAGIC #### 4.0.1. Dataframe API

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.1.1. Read using session.read.format("org.apache.spark.sql.cassandra")

// COMMAND ----------

val readBooksDF = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .load

readBooksDF.explain
readBooksDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.1.2. Read using spark.read with read.cassandraFormat(...)

// COMMAND ----------

val readBooksDF = spark
  .read
  .cassandraFormat("books", "books_ks", "")
  .load().show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.1.3. Projection and predicate pushdowns
// MAGIC Note: Predicate pushdown is not supported yet.  Any filtering is on the client-side(Spark)

// COMMAND ----------

val readBooksDF = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .load
  .select("book_name","book_author", "book_pub_year")
  .filter("book_pub_year > 1891")
//.filter("book_name IN ('A sign of four','A study in scarlet')")
//.filter("book_name='A sign of four' OR book_name='A study in scarlet'")
//.filter("book_author='Arthur Conan Doyle' AND book_pub_year=1890")
//.filter("book_pub_year=1903")  

readBooksDF.printSchema
readBooksDF.explain
readBooksDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.0.2. RDD API

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.2.1. Read 

// COMMAND ----------

val bookRDD = sc.cassandraTable("books_ks", "books")
bookRDD.take(5).foreach(println)

// COMMAND ----------

//Limit results
val bookRDD = sc.cassandraTable("books_ks", "books").limit(1).collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.2.2. Projection pushdown

// COMMAND ----------

val booksRDD = sc.cassandraTable("books_ks", "books").select("book_id","book_name").cache
booksRDD.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4.0.2.3. Predicate pushdown
// MAGIC Not supported yet

// COMMAND ----------

//sc.cassandraTable("books_ks", "books").select("book_id","book_name").where("book_name = ?", "A sign of four").take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ####4.0.3 Create view and read off of the same

// COMMAND ----------

spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .load.createOrReplaceTempView("books_vw")

// COMMAND ----------

// MAGIC %md
// MAGIC **Explore the data running queries below:**

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from books_vw;
// MAGIC --select * from books_vw where book_pub_year > 1891