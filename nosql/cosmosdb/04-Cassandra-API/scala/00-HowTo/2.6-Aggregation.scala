// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC Basics of how to work with Azure Cosmos DB - Cassandra API from Databricks <B>in batch</B>.<BR>
// MAGIC Section 07: Aggregation operations<BR>
// MAGIC   
// MAGIC **NOTE:**<br>
// MAGIC 1) Server-side(Cassandra) filtering of non-partition key columns is not yet supported.<BR>
// MAGIC 2) Server-side(Cassandra) aggregation operations are not yet supported yet<BR>
// MAGIC The samples below perform the same on the Spark-side<br>

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
// MAGIC ## Data generator

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
// MAGIC ## 7.0. Aggregation operations

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.0.1. Count

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.1a. RDD API

// COMMAND ----------

sc.cassandraTable("books_ks", "books").count

// COMMAND ----------

//count on cassandra side - NOT SUPPORTED YET
//sc.cassandraTable("books_ks", "books").cassandraCount

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.1b. Dataframe API
// MAGIC *Roadmap item for dataframe (works for RDD)*<br>
// MAGIC While we are pending release of count support, the sample below shows how we can execute counts currently -<br>
// MAGIC - materializes the dataframe to memory and then does a count<BR>
// MAGIC   
// MAGIC 
// MAGIC **Options for storage level**<br>
// MAGIC https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#which-storage-level-to-choose<br>
// MAGIC (1) MEMORY_ONLY:	
// MAGIC Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level.<br>
// MAGIC (2) MEMORY_AND_DISK:	<br>
// MAGIC Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when they're needed.<br>
// MAGIC (3) MEMORY_ONLY_SER: Java/Scala<br>
// MAGIC Store RDD as serialized Java objects (one byte array per partition). This is generally more space-efficient than deserialized objects, especially when using a fast serializer, but more CPU-intensive to read.<br>
// MAGIC (4) MEMORY_AND_DISK_SER:  Java/Scala<br>
// MAGIC Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of recomputing them on the fly each time they're needed.<br>
// MAGIC (5) DISK_ONLY:	<br>
// MAGIC Store the RDD partitions only on disk.<br>
// MAGIC (6) MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc.	<br>
// MAGIC Same as the levels above, but replicate each partition on two cluster nodes.<br>
// MAGIC (7) OFF_HEAP (experimental):<br>
// MAGIC Similar to MEMORY_ONLY_SER, but store the data in off-heap memory. This requires off-heap memory to be enabled.<br>

// COMMAND ----------

//Workaround
import org.apache.spark.storage.StorageLevel

//Read from source
val readBooksDF = spark
  .read
  .cassandraFormat("books", "books_ks", "")
  .load()

//Explain plan
readBooksDF.explain

//Materialize the dataframe
readBooksDF.persist(StorageLevel.MEMORY_ONLY)

//Subsequent execution against this DF hits the cache 
readBooksDF.count

//Persist as temporary view
readBooksDF.createOrReplaceTempView("books_vw")

// COMMAND ----------

// MAGIC %sql
// MAGIC --Quick look at data
// MAGIC select * from books_vw

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from books_vw where book_pub_year > 1900;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(book_id) from books_vw;

// COMMAND ----------

// MAGIC %sql
// MAGIC select book_author, count(*) as count from books_vw group by book_author;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from books_vw;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.0.2. Average

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.2a. SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC select avg(book_price) from books_vw;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.2b. RDD

// COMMAND ----------

sc.cassandraTable("books_ks", "books").select("book_price").as((c: Double) => c).mean

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.2c. Dataframe

// COMMAND ----------

spark
  .read
  .cassandraFormat("books", "books_ks", "")
  .load()
  .select("book_price")
  .agg(avg("book_price"))
  .show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.0.3. Min

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.3.a. SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC select min(book_price) from books_vw;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.3.b. RDD API

// COMMAND ----------

sc.cassandraTable("books_ks", "books").select("book_price").as((c: Float) => c).min

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.3.c. Dataframe API

// COMMAND ----------

spark
  .read
  .cassandraFormat("books", "books_ks", "")
  .load()
  .select("book_id","book_price")
  .agg(min("book_price"))
  .show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.0.4. Max

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.4.a. SQL API

// COMMAND ----------

// MAGIC %sql
// MAGIC select max(book_price) from books_vw;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.4.b. RDD API

// COMMAND ----------

sc.cassandraTable("books_ks", "books").select("book_price").as((c: Float) => c).max

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.4.c. Dataframe API

// COMMAND ----------

spark
  .read
  .cassandraFormat("books", "books_ks", "")
  .load()
  .select("book_price")
  .agg(max("book_price"))
  .show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.0.5. Sum

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.5.a. SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC select sum(book_price) from books_vw;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.5.b. Dataframe API

// COMMAND ----------

spark
  .read
  .cassandraFormat("books", "books_ks", "")
  .load()
  .select("book_price")
  .agg(sum("book_price"))
  .show

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.5.b. RDD API

// COMMAND ----------

sc.cassandraTable("books_ks", "books").select("book_price").as((c: Float) => c).sum

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.0.6. Top

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.6.a. SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC select book_name,book_price from books_vw order by book_price desc limit 3;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.6.a. RDD API

// COMMAND ----------

val readCalcTopRDD = sc.cassandraTable("books_ks", "books").select("book_name","book_price").sortBy(_.getFloat(1), false)
readCalcTopRDD.zipWithIndex.filter(_._2 < 3).collect.foreach(println)
//delivers the first top n items without collecting the rdd to the driver.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7.0.6.c. Dataframe API

// COMMAND ----------

import org.apache.spark.sql.functions._

val readBooksDF = spark.read
.format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "books", "keyspace" -> "books_ks"))
  .load
  .select("book_name","book_price")
  .orderBy(desc("book_price"))
  .limit(3)

//Explain plan
readBooksDF.explain

//Top 3
readBooksDF.show