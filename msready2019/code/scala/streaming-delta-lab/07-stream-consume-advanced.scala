// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this notebook?
// MAGIC 1.  Learn to read a stream from event hub and perform near real time computation on the stream - aggregates with/without windowing, stream to static data joins

// COMMAND ----------

// MAGIC %run ./01-common

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.0. Streaming data to static data join

// COMMAND ----------

import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._

// 1) AEH consumer related
val aehConsumerParams =
  EventHubsConf(aehConexionCreds)
  .setConsumerGroup("nyc-aeh-topic-streaming-cg")
  .setStartingPosition(EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(500)

// COMMAND ----------

// 2) Read the stream 
val incomingStreamingDF = spark.readStream.format("eventhubs").options(aehConsumerParams.toMap).load()
incomingStreamingDF.printSchema
    
// 3) Body is binary, so we cast it to string to see the actual content of the message
val tripsOnlyStreamingDF = incomingStreamingDF.withColumn("json_payload", $"body".cast(StringType))

// 4) Parse events
import org.apache.spark.sql.functions._

val consumableStreamingDF = tripsOnlyStreamingDF.select(get_json_object($"json_payload", "$.vendor_id").alias("vendor_id"),
                                          get_json_object($"json_payload", "$.pickup_datetime").cast(TimestampType).alias("pickup_datetime"),
                                          get_json_object($"json_payload", "$.dropoff_datetime").cast(TimestampType).alias("dropoff_datetime"),
                                          get_json_object($"json_payload", "$.passenger_count").alias("passenger_count"),
                                          get_json_object($"json_payload", "$.trip_distance").alias("trip_distance"),
                                          get_json_object($"json_payload", "$.rate_code_id").alias("rate_code_id"),
                                          get_json_object($"json_payload", "$.store_and_fwd_flag").alias("store_and_fwd_flag"),
                                          get_json_object($"json_payload", "$.pickup_locn_id").alias("pickup_locn_id"),
                                          get_json_object($"json_payload", "$.dropoff_locn_id").alias("dropoff_locn_id"),
                                          get_json_object($"json_payload", "$.payment_type").alias("payment_type"),
                                          get_json_object($"json_payload", "$.fare_amount").cast(FloatType).alias("fare_amount"),
                                          get_json_object($"json_payload", "$.extra").cast(FloatType).alias("extra"),
                                          get_json_object($"json_payload", "$.mta_tax").cast(FloatType).alias("mta_tax"),
                                          get_json_object($"json_payload", "$.tip_amount").cast(FloatType).alias("tip_amount"),
                                          get_json_object($"json_payload", "$.tolls_amount").cast(FloatType).alias("tolls_amount"),
                                          get_json_object($"json_payload", "$.improvement_surcharge").cast(FloatType).alias("improvement_surcharge"),
                                          get_json_object($"json_payload", "$.total_amount").cast(FloatType).alias("total_amount"))

consumableStreamingDF.printSchema

// COMMAND ----------

// 5) Console output of parsed events for testing only
// consumableStreamingDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember to:** Cancel the command above once you see a table displayed with results and see it updating.  We will need our cluster resources available for subsequent steps.

// COMMAND ----------

// MAGIC %md **Note:** Event Hubs has a small internal buffer (~256kB by default). Due to this reason, the incoming stream for successive queries might be slow or take some time to load. Please wait for a couple of minutes after firing your query to see the results appear. 

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.0. Near real-time/streaming aggregations

// COMMAND ----------

// MAGIC %md  ### 2.0.1: "Count" of passengers by pick-up location
// MAGIC 
// MAGIC We will leverage 'groupBy' on pickup_locn_id to get a moving count of passengers, leveraging the aggregate sum function on passenger_count.

// COMMAND ----------

val passengerCountByPickupLocationDS = (
    consumableStreamingDF
   .groupBy($"pickup_locn_id")
   .agg(sum($"passenger_count").as("passenger_count"))
   .orderBy($"passenger_count".desc))

display(passengerCountByPickupLocationDS)

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember to:** Cancel the command above once you see a table displayed with results and see it updating.  We will need our cluster resources available for subsequent steps.

// COMMAND ----------

// MAGIC %md  ### 2.0.2: "Average" fare by vendor
// MAGIC 
// MAGIC We will leverage 'groupBy' on vendor_id to get average fare, leveraing the aggregate average function on total_amount to get the average amount earned by vendor.

// COMMAND ----------

val averageFareByVendorDS = (
    consumableStreamingDF
   .groupBy($"vendor_id")
   .agg(avg($"total_amount").as("average_amount"))
   .orderBy($"average_amount".desc)
)

display(averageFareByVendorDS)

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember to:** Cancel the command above once you see a table displayed with results and see it updating.  We will need our cluster resources available for subsequent steps.

// COMMAND ----------

// MAGIC %md ### 2.0.3: Mulitple aggregates in one output###
// MAGIC 
// MAGIC Uses groupBy on VendorID and pickup date and compute a dataset that includes aggregate sum of passenger_count, trip_distance and total_amount and ordered by the pickup date

// COMMAND ----------

val multiAggrDS = (    
    consumableStreamingDF
   .groupBy($"vendor_id",to_date($"pickup_datetime").alias("pickup_date"))
   .agg(sum($"passenger_count").as("total_passenger"),sum($"trip_distance").as("total_distance"),sum($"total_amount").as("total_amount"))
   .orderBy($"pickup_date")
  )

display(multiAggrDS)

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember to:** Cancel the command above once you see a table displayed with results and see it updating.  We will need our cluster resources available for subsequent steps.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3.0. Window operations

// COMMAND ----------

// MAGIC %md ### 3.0.1. Sliding window of top pickup locations with the highest passenger count, from the past 10 seconds, computed every 5 seconds

// COMMAND ----------

val topPickupLocationsByWindowDS = (
    consumableStreamingDF
   .groupBy($"pickup_locn_id",window($"pickup_datetime", "10 second", "5 second"))
   .agg(sum($"passenger_count").as("passenger_count"))
   .orderBy($"window".desc,$"passenger_count".desc)
   .select("pickup_locn_id","window.start","window.end","passenger_count")
)

display(topPickupLocationsByWindowDS)

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember to:** Cancel the command above once you see a table displayed with results and see it updating.  We will need our cluster resources available for subsequent steps.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4.0. Stream join with static reference data

// COMMAND ----------

// MAGIC %md
// MAGIC Joining streams with static data is no different from joining two dataframes with static data.<br>
// MAGIC In this section, we will join our stream with the taxi zone dataset that we curated earlier in the lab.

// COMMAND ----------

// 1.  Load dataframe with reference data (taxi zone)
val zoneStaticRefDataDF = spark.sql("select * from taxi_db.taxi_zone_lookup")

// COMMAND ----------

//2.  Join streaming dataframe with static zone data for augmenting with pickup zone information
val augmentedIntialStreamingDF = (
    zoneStaticRefDataDF
   .select($"location_id" as "pickup_locn_id"
          ,$"borough" as "pickup_borough"
          ,$"zone" as "pickup_zone"
          ,$"service_zone" as "pickup_service_zone"
         )
   .join(consumableStreamingDF,"pickup_locn_id")
 ) 

//3.  Join streaming dataframe with static zone data for augmenting with dropoff zone information
val augmentedFinalStreamingDF = (
    zoneStaticRefDataDF
   .select($"location_id" as "dropoff_locn_id"
          ,$"borough" as "dropoff_borough"
          ,$"zone" as "dropoff_zone"
          ,$"service_zone" as "dropoff_service_zone" 
         )   
   .join(augmentedIntialStreamingDF,"dropoff_locn_id")
  )

//4.  Display
display(augmentedFinalStreamingDF)

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember to:** Cancel the command above once you see a table displayed with results and see it updating.  We will need our cluster resources available for subsequent steps.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 5.0. Persist stream to Databricks Delta

// COMMAND ----------

//1.  Join streaming dataframe with static zone data for augmenting with pickup zone information
val augmentedIntialStreamingDF = (
    zoneStaticRefDataDF
   .select($"location_id" as "pickup_locn_id"
          ,$"borough" as "pickup_borough"
          ,$"zone" as "pickup_zone"
          ,$"service_zone" as "pickup_service_zone"
         )
   .join(consumableStreamingDF,"pickup_locn_id")
 ) 

//2.  Join streaming dataframe with static zone data for augmenting with dropoff zone information
val augmentedFinalStreamingDF = (
    zoneStaticRefDataDF
   .select($"location_id" as "dropoff_locn_id"
          ,$"borough" as "dropoff_borough"
          ,$"zone" as "dropoff_zone"
          ,$"service_zone" as "dropoff_service_zone" 
         )   
   .join(augmentedIntialStreamingDF,"dropoff_locn_id")
  )

//3.  Persist to Databricks Delta

//Destination directory for delta table, and for checkpoints
val dbfsDestDirPath="/mnt/workshop/curated/transactions/"

//AEH checkpoint related
val dbfsCheckpointDirPath="/mnt/workshop/scratch/checkpoints-aeh-sub/"

//Remove output from prior execution
//dbutils.fs.rm(dbfsDestDirPath, recurse=true)
// dbutils.fs.rm(dbfsCheckpointDirPath, recurse=true)

//Persist as delta format (Parquet) to curated zone to a delta table
augmentedFinalStreamingDF.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", dbfsCheckpointDirPath)
  .start(dbfsDestDirPath)