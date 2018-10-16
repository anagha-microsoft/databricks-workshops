// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 2 of 4 notebooks that demonstrate stream ingest from Kafka, of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In notebook 1, we published data to Kafka for purpose of the exercise<BR>
// MAGIC - In notebook 2, we attempted to ingest from Kafka using structured stream processing and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In **this notebook**, we will ingest from Kafka using classic/legacy stream processing (DStream based) and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 4, we will ingest from Kafka using structured stream processing and persist to a Databricks Delta table<BR>
// MAGIC   
// MAGIC While the Azure Cosmos DB Cassandra table serves as a hot store for OLTP, the Delta table will serve as an analytics store.<BR><BR>
// MAGIC Reference for Databricks 4.3, Spark 2.3.1, Scala 2.11, Kafka : [Spark docs for spark streaming](https://spark.apache.org/docs/2.3.1/streaming-programming-guide.html) | [Spark docs specific to Kafka integration](https://spark.apache.org/docs/2.3.1/streaming-kafka-0-10-integration.html)

// COMMAND ----------

package com.microsoft.cassandraapi.parsers 

import org.apache.spark.sql.Row
object CrimeParser {
  case class ChicagoCrime(
    case_id: Int, 
    case_nbr: String, 
    case_dt_tm: String,
    block: String,
    iucr: String,
    primary_type: String,
    description: String,
    location_description: String,
    arrest_made: Boolean,
    was_domestic: Boolean,
    beat: Int,
    district: Int,
    ward: Int,
    community_area: Int,
    fbi_code: String,
    x_coordinate: Int,
    y_coordinate: Int,
    year: Int,
    updated_dt: String,
    latitude: Double,
    longitude: Double,
    location_coordinates: String,
    case_timestamp: java.sql.Timestamp,
    case_month: Int,
    case_day_of_month: Int,
    case_hour: Int,
    case_day_of_week_nbr: Int,
    case_day_of_week_name: String)

  object ChicagoCrime {
    def apply(r: Row): ChicagoCrime = ChicagoCrime(
      r.getInt(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), r.getString(6), r.getString(7), r.getBoolean(8), r.getBoolean(9), r.getInt(10), r.getInt(11), r.getInt(12), r.getInt(13), r.getString(14), r.getInt(15), r.getInt(16), r.getInt(17), r.getString(18), r.getDouble(19), r.getDouble(20), r.getString(21), r.getTimestamp(22), r.getInt(23), 
      r.getInt(24), r.getInt(25), r.getInt(26), r.getString(27))
  }
} 

// COMMAND ----------

//datastax Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.streaming._

//CosmosDB library for multiple retry
import com.microsoft.azure.cosmosdb.cassandra

//Kafka related
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

//Other
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import spark.implicits._

//import com.microsoft.cassandraapi.parsers 

//Specify connection factory for Cassandra
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

//Write tuning parameters
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")//Leave this as one for Cosmos DB
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "2")//Maximum number of connections per Host set on each Executor JVM - default parallelism/executors for Spark Commands
spark.conf.set("spark.cassandra.output.concurrent.writes", "2")//Maximum number of batches executed in parallel by a single Spark task
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "300")//How many batches per single Spark task can be stored in memory before sending to Cassandra
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "5000") //Period of time to keep unused connections open

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Read from Kafka with spark classic (legacy) streaming (DStream API)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.1. Topic, broker list, checkpoint directory

// COMMAND ----------

//1) Topic and broker list
val crimesKafkaTopicArray = Array("crimes_chicago")
//Replace the below with specifics for your broker instances
val crimesKafkaBrokerAndPortCSV = "10.7.0.12:9092,10.7.0.13:9092,10.7.0.14:9092,10.7.0.15:9092"

//2) Destination directory for checkpoints
val dbfsCheckpointDirPath="/mnt/data/crimes/checkpointDir/chicago-crimes-stream-cassandra-kafka/"
dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)
val dbfsDestDirPath="/mnt/data/crimes/curatedDir/chicago-crimes-stream-cassandra-kafka-legacy/"
dbutils.fs.rm(dbfsDestDirPath, recurse=true)

//3) Other streaming related conf
val streamingBatchIntervalSeconds = 5
val streamingMaxRateOfIngest = "1000" //Max rate of ingest from Kafka - needs to be a string

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.2. Create a Direct DStream

// COMMAND ----------

import java.util.UUID

val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> crimesKafkaBrokerAndPortCSV,
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "auto.offset.reset" -> "earliest",
  "group.id" -> UUID.randomUUID().toString,
  "enable.auto.commit" -> (false: java.lang.Boolean)
)

val batchInterval = Seconds(streamingBatchIntervalSeconds)
val ssc = new StreamingContext(sc, batchInterval)

val crimesRawDstream = KafkaUtils.createDirectStream[String, String](
  ssc,
  PreferConsistent,
  Subscribe[String, String](crimesKafkaTopicArray, kafkaParams)
)

//Accumulator for crime count
val totalCrimeCount = ssc.sparkContext.longAccumulator("Number of crimes received")
//Counter for elapsed time in seconds
var secondCounter: Long = streamingBatchIntervalSeconds


// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.0.3. Parse and persist

// COMMAND ----------

//Extract only the payload
val crimesDstream = crimesRawDstream.map(record =>  (record.value))

//Parse json and persist to Cosmos DB
crimesDstream.foreachRDD { rdd =>
      val rddCrimeCount = rdd.count()
      totalCrimeCount.add(rddCrimeCount)
      println(s"Total logs for the batch: $rddCrimeCount")
  
      if (!rdd.isEmpty()) {
        //Parse the JSON into a DF
        val consumableDF = rdd.toDF("json_payload").select(get_json_object($"json_payload", "$.case_id").cast(IntegerType).alias("case_id"),
                                          get_json_object($"json_payload", "$.case_nbr").alias("case_nbr"),
                                          get_json_object($"json_payload", "$.case_dt_tm").alias("case_dt_tm"),
                                          get_json_object($"json_payload", "$.block").alias("block"),
                                          get_json_object($"json_payload", "$.iucr").alias("iucr"),
                                          get_json_object($"json_payload", "$.primary_type").alias("primary_type"),
                                          get_json_object($"json_payload", "$.description").alias("description"),
                                          get_json_object($"json_payload", "$.location_description").alias("location_description"),
                                          get_json_object($"json_payload", "$.arrest_made").cast(BooleanType).alias("arrest_made"),
                                          get_json_object($"json_payload", "$.was_domestic").cast(BooleanType).alias("was_domestic"),
                                          get_json_object($"json_payload", "$.beat").cast(IntegerType).alias("beat"),
                                          get_json_object($"json_payload", "$.district").cast(IntegerType).alias("district"),
                                          get_json_object($"json_payload", "$.ward").cast(IntegerType).alias("ward"),
                                          get_json_object($"json_payload", "$.fbi_code").alias("fbi_code"),
                                          get_json_object($"json_payload", "$.x_coordinate").cast(IntegerType).alias("x_coordinate"),
                                          get_json_object($"json_payload", "$.y_coordinate").cast(IntegerType).alias("y_coordinate"),
                                          get_json_object($"json_payload", "$.case_year").cast(IntegerType).alias("case_year"),
                                          get_json_object($"json_payload", "$.updated_dt").alias("updated_dt"),
                                          get_json_object($"json_payload", "$.latitude").cast(DoubleType).alias("latitude"),
                                          get_json_object($"json_payload", "$.longitude").cast(DoubleType).alias("longitude"),
                                          get_json_object($"json_payload", "$.location_coords").alias("location_coords"),
                                          get_json_object($"json_payload", "$.case_timestamp").cast(TimestampType).alias("case_timestamp"),
                                          get_json_object($"json_payload", "$.case_month").cast(IntegerType).alias("case_month"),
                                          get_json_object($"json_payload", "$.case_day_of_month").cast(IntegerType).alias("case_day_of_month"),          
                                          get_json_object($"json_payload", "$.case_hour").cast(IntegerType).alias("case_hour"),
                                          get_json_object($"json_payload", "$.case_day_of_week_nbr").cast(IntegerType).alias("case_day_of_week_nbr"),
                                          get_json_object($"json_payload", "$.case_day_of_week_name").alias("case_day_of_week_name")
                                          )
        
          
          consumableDF.write.mode("append").format("org.apache.spark.sql.cassandra").options(Map( "table" -> "crimes_chicago_stream_kafka", "keyspace" -> "crimes_ks")).save()

      }//if (!rdd.isEmpty()) 
      println(s"TOTAL CRIMES TO DATE FROM THE PAST " + secondCounter + " SECONDS: " + totalCrimeCount)
      println("=================================================================================")
      secondCounter = secondCounter + streamingBatchIntervalSeconds
}
crimesDstream.print
ssc.start()
ssc.awaitTermination()

// COMMAND ----------

//ssc.stop()
