// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise
// MAGIC This is part 2 of 4 notebooks that demonstrate stream ingest from Kafka, of 1.5 GB of the Chicago crimes public dataset.<BR>
// MAGIC - In notebook 1, we published data to Kafka for purpose of the exercise<BR>
// MAGIC - In notebook 2, we attempted to ingest from Kafka using structured stream processing and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In **this notebook**, we will ingest from Kafka using classic/legacy stream processing and persist to an Azure Cosmos DB Cassandra table<BR>
// MAGIC - In notebook 4, we will ingest from Kafka using structured stream processing and persist to a Databricks Delta table<BR>
// MAGIC   
// MAGIC While the Azure Cosmos DB Cassandra table serves as a hot store for OLTP, the Delta table will serve as an analytics store.<BR><BR>
// MAGIC Reference for Databricks 4.3, Spark 2.3.1, Scala 2.11, Kafka : [Spark docs for spark streaming](https://spark.apache.org/docs/2.3.1/streaming-programming-guide.html) | [Spark docs specific to Kafka integration](https://spark.apache.org/docs/2.3.1/streaming-kafka-0-10-integration.html)

// COMMAND ----------

//Cassandra connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.streaming._

//CosmosDB library for multiple retry to Cassandra
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
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import spark.implicits._
import java.util.UUID

//Specify connection factory for Cassandra
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

//Write tuning parameters
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")//Leave this as one for Cosmos DB
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "2")//Maximum number of connections per Host set on each Executor JVM - default parallelism/executors for Spark Commands
spark.conf.set("spark.cassandra.output.concurrent.writes", "2")//Maximum number of batches executed in parallel by a single Spark task
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "300")//How many batches per single Spark task can be stored in memory before sending to Cassandra
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "5000") //Period of time to keep unused connections open

// COMMAND ----------

val streamingBatchIntervalSeconds = 5
val streamingMaxRateOfIngest = "1000" //Max rate of ingest from Kafka - needs to be a string
val crimesKafkaTopicArray = Array("crimes_chicago")
val crimesKafkaBrokerAndPortCSV = "10.7.0.12:9092,10.7.0.13:9092,10.7.0.14:9092,10.7.0.15:9092" //Replace with your broker instances
val checkPointDirectory = "/mnt/data/crimes/checkpointDir/chicago-crimes-stream-kafka-consumer/"
val startDateTime =  java.time.LocalDateTime.now //Current date and time

// COMMAND ----------

def createSparkStreamingContext(
    checkpointDirectory: String,
    batchIntervalSeconds: Int,
    maxRateOfIngest: String,
    applicationName: String,
    sparkSession: SparkSession): StreamingContext = {

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(batchIntervalSeconds))
    //ssc.checkpoint(checkpointDirectory)
    ssc
  }

// COMMAND ----------

//val ssc = new StreamingContext(sc, Seconds(streamingBatchIntervalSeconds))
dbutils.fs.rm(checkPointDirectory, recurse=true) //In case you need to
val ssc = StreamingContext.getOrCreate(checkPointDirectory,() => createSparkStreamingContext(checkPointDirectory,streamingBatchIntervalSeconds,
        streamingMaxRateOfIngest, "CrimesIngestionFromKafkaSample", spark))

val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> crimesKafkaBrokerAndPortCSV,
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "auto.offset.reset" -> "latest",
  "group.id" -> UUID.randomUUID().toString,
  "enable.auto.commit" -> (false: java.lang.Boolean))

val crimesRawDstream = KafkaUtils.createDirectStream[String, String](
  ssc,
  PreferConsistent,
  Subscribe[String, String](crimesKafkaTopicArray, kafkaParams))

val crimesDstream = crimesRawDstream.map(record =>  (record.value)) //Only payload

//Accumulator for crime count
val totalCrimeCount = ssc.sparkContext.longAccumulator("Number of crimes received")
//Counter for elapsed time in seconds
//var secondCounter: Long = streamingBatchIntervalSeconds

// COMMAND ----------

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

      }
      println(s"TOTAL CRIMES FROM " + startDateTime + " TO " + java.time.LocalDateTime.now + " is: " + totalCrimeCount)
      println("=================================================================================")
}
crimesDstream.print
ssc.start()
ssc.awaitTermination()

// COMMAND ----------

//Need this if I ever need to rerun after clearing state
//ssc.stop()
