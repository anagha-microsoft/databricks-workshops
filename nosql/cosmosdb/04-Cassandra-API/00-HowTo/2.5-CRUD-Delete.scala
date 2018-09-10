// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC Basics of how to work with CosmosDB from Databricks <B>in batch</B>.<BR>
// MAGIC Section 06: Delete operation (cRud)<BR>
// MAGIC 
// MAGIC **NOTE:**<br>
// MAGIC Server-de(Cassandra) filtering of non-partition key columns is not supported yet.<BR>
// MAGIC You will see caching done at the Spark level as a workaround till we release server side filtering.<br> 
// MAGIC 
// MAGIC **Reference:**<br> 
// MAGIC **TODO**

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6.0. Delete operation

// COMMAND ----------

// MAGIC %run ../00-HowTo/2-CassandraUtils

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
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "600000000") //Increase this number as needed

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Data generator for testing this module

// COMMAND ----------

// Generate a simple dataset containing five values and
val booksDF = Seq(
   ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887,11.33),
   ("b00023", "Arthur Conan Doyle", "A sign of four", 1890,22.45),
   ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892,19.83),
   ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893,14.22),
   ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901,12.25)
).toDF("book_id", "book_author", "book_name", "book_pub_year","book_price")

booksDF.write
  .mode("append")
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks", "output.consistency.level" -> "ALL", "ttl" -> "10000000"))
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6.0.1. Delete rows based on a condition

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 6.0.1a. RDD API

// COMMAND ----------

// MAGIC %md
// MAGIC Not supported until predicate pushdown is supported

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 6.0.1b. Dataframe API

// COMMAND ----------

//1) Create dataframe
val deleteBooksDF = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .load
  .filter("book_pub_year = 1887")

//2) Review execution plan
deleteBooksDF.explain

//3) Review table data before execution
println("==================")
println("1) Before")
deleteBooksDF.show
println("==================")

//4) Delete selected records in dataframe
println("==================")
println("2a) Starting delete")

//Reuse connection for each partition
var deleteString = ""
val cdbConnector = CassandraConnector(sc)
deleteBooksDF.foreachPartition(partition => {
  cdbConnector.withSessionDo(session =>
    partition.foreach{ book => 
        val delete = s"DELETE FROM books_ks.books where book_id='"+book.getString(0) +"';"
        session.execute(delete)
    })
})

println("2b) Completed delete")
println("deleteString" +  deleteString)
println("==================")

//5) Review table data after delete operation
println("3) After")
spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .load
  .show

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6.0.2. Delete all rows in a table

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 6.0.2a. RDD API

// COMMAND ----------

//1) Create RDD with specific rows to delete
val deleteBooksRDD = 
    sc.cassandraTable("books_ks", "books")

//2) Review table data before execution
println("==================")
println("1) Before")
deleteBooksRDD.collect.foreach(println)
println("==================")

//3) Delete selected records in dataframe
println("==================")
println("2a) Starting delete")

/* Option 1: 
// Does not work as of Sep - throws error
sc.cassandraTable("books_ks", "books")
  .where("book_pub_year = 1891")
  .deleteFromCassandra("books_ks", "books")
*/

//Option 2: CassandraConnector and CQL
//Reuse connection for each partition
val cdbConnector = CassandraConnector(sc)
deleteBooksRDD.foreachPartition(partition => {
    cdbConnector.withSessionDo(session =>
    partition.foreach{book => 
        val delete = s"DELETE FROM books_ks.books where book_id='"+ book.getString(0) +"';"
        session.execute(delete)
    }
   )
})

println("Completed delete")
println("==================")

println("2b) Completed delete")
println("==================")

//5) Review table data after delete operation
println("3) After")
sc.cassandraTable("books_ks", "books").collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 6.0.2b. Dataframe API

// COMMAND ----------

//1) Create dataframe
val deleteBooksDF = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .load

//2) Review execution plan
deleteBooksDF.explain

//3) Review table data before execution
println("==================")
println("1) Before")
deleteBooksDF.show
println("==================")

//4) Delete selected records in dataframe
println("==================")
println("2a) Starting delete")

//Reuse connection for each partition
var deleteString = ""
val cdbConnector = CassandraConnector(sc)
deleteBooksDF.foreachPartition(partition => {
  cdbConnector.withSessionDo(session =>
    partition.foreach{ book => 
        deleteString = deleteString + "DELETE FROM books_ks.books where book_id='"+book.getString(0) +"';"
      println(deleteString)
        val delete = s"DELETE FROM books_ks.books where book_id='"+book.getString(0) +"';"
        session.execute(delete)
    })
})

println("2b) Completed delete")
println("deleteString" +  deleteString)
println("==================")

//5) Review table data after delete operation
println("3) After")
spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .load
  .show

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6.0.3. Delete specific column only

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 6.0.3a. RDD API

// COMMAND ----------

// Generate a simple dataset containing five values including book_price
val booksDF = Seq(
   ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887,23),
   ("b00023", "Arthur Conan Doyle", "A sign of four", 1890,11),
   ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892,10),
   ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893,5),
   ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901,20)
).toDF("book_id", "book_author", "book_name", "book_pub_year","book_price")

booksDF.write
  .mode("append")
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks", "output.consistency.level" -> "ALL", "ttl" -> "10000000"))
  .save()

// COMMAND ----------

//1) Create RDD with specific rows to delete
val deleteBooksRDD = 
    sc.cassandraTable("books_ks", "books")

//2) Review table data before execution
println("==================")
println("1) Before")
deleteBooksRDD.collect.foreach(println)
println("==================")

//3) Delete selected records in dataframe
println("==================")
println("2a) Starting delete of book price")

sc.cassandraTable("books_ks", "books")
  .deleteFromCassandra("books_ks", "books",SomeColumns("book_price"))

println("Completed delete")
println("==================")

println("2b) Completed delete")
println("==================")

//5) Review table data after delete operation
println("3) After")
sc.cassandraTable("books_ks", "books").collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 6.0.3b. Dataframe API

// COMMAND ----------

// Generate a simple dataset containing five values including book_price
val booksDF = Seq(
   ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887,23),
   ("b00023", "Arthur Conan Doyle", "A sign of four", 1890,11),
   ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892,10),
   ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893,5),
   ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901,20)
).toDF("book_id", "book_author", "book_name", "book_pub_year","book_price")

booksDF.write
  .mode("append")
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks", "output.consistency.level" -> "ALL"))
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC **TODO**

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6.0.4. Delete rows older than a specific timestamp

// COMMAND ----------

val cdbConnector = CassandraConnector(sc)

cdbConnector.withSessionDo(session => session.execute("CREATE TABLE IF NOT EXISTS books_ks.books_with_ts(book_id TEXT PRIMARY KEY,book_author TEXT, book_name TEXT,book_pub_year INT,book_price FLOAT,insert_date timestamp) WITH cosmosdb_provisioned_throughput=4000;"))

// COMMAND ----------

// Generate a simple dataset containing five values including book_price
val booksDF = Seq(
   ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887,23,"2017-09-10 23:12:18.045"),
   ("b00023", "Arthur Conan Doyle", "A sign of four", 1890,11,"2017-09-10 23:12:18.045"),
   ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892,10,"2017-09-10 23:12:18.045"),
   ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893,5,"2017-09-10 23:12:18.045"),
   ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901,20,"2018-09-10 23:12:18.045")
).toDF("book_id", "book_author", "book_name", "book_pub_year","book_price","insert_date")

booksDF.show()

booksDF.write
  .mode("append")
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books_with_ts", "keyspace" -> "books_ks", "output.consistency.level" -> "ALL"))
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #The below is WIP

// COMMAND ----------

import com.datastax.spark.connector.writer._
import java.sql.Date
import java.util.Calendar

//Read table
val deleteBooksRDD = 
    sc.cassandraTable("books_ks", "books_with_ts")

//Delete timestamp
val deleteTimestamp = "2018-09-09 23:12:18.045"

//Delete operation
deleteBooksRDD.deleteFromCassandra(
  "books_ks",
  "books",
  writeConf = WriteConf(timestamp = (com.datastax.spark.connector.writer.TimestampOption)deleteTimestamp))

//Validate
spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books_with_ts", "keyspace" -> "books_ks"))
  .load
  .show

// COMMAND ----------





// COMMAND ----------

sc.cassandraTable("books_ks", "books").collect().foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6.0.5. Delete a range of keys

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6.0.6. Expire rows

// COMMAND ----------

