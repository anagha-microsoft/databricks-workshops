// Databricks notebook source
// MAGIC %md ## Publish trips in Azure Blob Storage to Azure Event Hub to create a streaming source for the lab
// MAGIC 
// MAGIC We downloaded the Taxi trip dataset and persisted it to the staging directory in out DBFS backed by Azre Blob Storage.<br>
// MAGIC We will read the same data and publish to Azure Event Hub - so we have a stream of data for our structured streaming lab.

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### About publishing events to an Azure Event Hub
// MAGIC 
// MAGIC Azure Event Hub expects the data published to be of the following format.
// MAGIC 
// MAGIC **Column and type** 
// MAGIC 1. body 	                 - *binary*
// MAGIC 2. partition 	             - *string*
// MAGIC 3. offset                    - *string*
// MAGIC 4. sequenceNumber            - *long*
// MAGIC 5. enqueuedTime              - *timestamp*
// MAGIC 6. publisher                 - *string*
// MAGIC 7. partitionKey              - *string*
// MAGIC 8. properties 	             - *map[string,json]*
// MAGIC 
// MAGIC The body is always provided as a byte array. Use cast("string") to explicitly deserialize the body column.
// MAGIC 
// MAGIC **The body column is the only required option**. If a partitionId and partitionKey are not provided, then events will distributed to partitions using a round-robin model.
// MAGIC Users can also provided properties via a map[string,json] if they would like to send any additional properties with their events.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Common variables

// COMMAND ----------

// MAGIC %run ./01-common

// COMMAND ----------

// MAGIC %md ### 2.0. Read trip data in blob storage into a dataframe

// COMMAND ----------

val tripDataDF = spark.read.option("header", true).csv("/mnt/workshop/staging/transactions/*yellow*.csv").limit(10000)
tripDataDF.printSchema
tripDataDF.show

// COMMAND ----------



// COMMAND ----------

import java.util._
import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//Set up Connection to Azure Event Hubs
val namespaceName = "nycTexiStream"
val eventHubName = "stream_data"
val sasKeyName = "shared_access_policy"
val sasKey = "q/RGz/sjPZMnhfXRh9YLNUyuSGh050zMHWNW9FxMuKM="
val connStr = new ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val pool = Executors.newFixedThreadPool(1)
val eventHubClient = EventHubClient.create(connStr.toString(), pool)

def sendEvent(message: String) = {
    val messageData = EventData.create(message.getBytes("UTF-8"))
        eventHubClient.get().send(messageData)
    }


// COMMAND ----------

import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Build connection string with the above information
val connectionString = ConnectionStringBuilder("Endpoint=sb://nyctexistream.servicebus.windows.net/;SharedAccessKeyName=shared_access_policy;SharedAccessKey=q/RGz/sjPZMnhfXRh9YLNUyuSGh050zMHWNW9FxMuKM=;EntityPath=stream_data")
      .setEventHubName("stream_data")
      .build

val customEventhubParameters =
    EventHubsConf(connectionString)
   .setMaxEventsPerTrigger(1) //set the maximun event at a time

// COMMAND ----------

// MAGIC %md We will create a function to send records to Event Hubs. This is done to simulate a realtime stream of events. Whenever we use our incoming stream, we can call `sendingEvents()` function to send fresh events so that our analysis is performed on a realtime stream.

// COMMAND ----------

def sendingData(count : Int) : Unit ={
  // Send stream to Event Hubs
  for( a <- 0 to (tripdata.length - 1)){
          sendEvent(tripdata(a).toString)
      }
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Read incoming data stream from Event Hubs ###
// MAGIC 
// MAGIC We will read the incoming stream using the `readStream` method of the Structured Streaming API.
// MAGIC 
// MAGIC The command window reports the status of the stream:
// MAGIC 
// MAGIC ![Stream Status](https://docs.databricks.com/_images/gsasg-stream-status.png)
// MAGIC 
// MAGIC When you expand `display_query`, you get a dashboard of the number of records processed, batch statistics, and the state of the aggregation:
// MAGIC 
// MAGIC ![Structured Streaming Dashboard](https://docs.databricks.com/_images/gsasg-streaming-dashboard.png)

// COMMAND ----------

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
    // Sending the incoming stream into the console.
    // Data comes in batches!
    // Event Hub message format is JSON and contains "body" field
    
// Body is binary, so we cast it to string to see the actual content of the message
val messages = incomingStream.withColumn("Body", $"body".cast(StringType))

sendingData(1)
display(messages)

// COMMAND ----------

// MAGIC %md **NOTE:** In case the stream dies midway during the course of this lab due to any failure of Event Hubs or Spark Stream Reader, you can go ahead and run `cmd 15` again in order to initiliase the stream again.

// COMMAND ----------

// MAGIC %md Our streaming data has the following information: Offset, Timestamp, readable Time and the Body. We will extract the Body from the stream as that contains our data and apply Spark Transformations on it to get it in the desired schema.
// MAGIC 
// MAGIC Also, in order to avoid failure of Spark Jobs if the incoming stream from Event Hubs is interrupted, we will write our stream to a Staging location in DBFS and read it from the Staging location for further analytics.

// COMMAND ----------

val messages1 = messages.select("Body") //select the body column which contains the data.
var cleanUDF = udf((s:String)=> {s.replaceAll("[\\[\\]]","")})

//Parse the data using ',' separator from the Body.  
val streamData = (
    messages1
   .withColumn("_tmp", split(cleanUDF($"Body"), ","))
   .select($"_tmp".getItem(0).as("VendorID")
          ,$"_tmp".getItem(1).cast("timestamp").as("tpep_pickup_datetime")
          ,$"_tmp".getItem(2).cast("timestamp").as("tpep_dropoff_datetime")
          ,$"_tmp".getItem(3).cast("int").as("passenger_count")
          ,$"_tmp".getItem(4).cast("float").as("trip_distance")
          ,$"_tmp".getItem(5).as("RatecodeID")
          ,$"_tmp".getItem(6).as("store_and_fwd_flag")
          ,$"_tmp".getItem(7).as("PULocationID")
          ,$"_tmp".getItem(8).as("DOLocationID")
          ,$"_tmp".getItem(9).as("payment_type")
          ,$"_tmp".getItem(10).cast("float").as("fare_amount")
          ,$"_tmp".getItem(11).cast("float").as("extra")
          ,$"_tmp".getItem(12).cast("float").as("mta_tax")
          ,$"_tmp".getItem(13).cast("float").as("tip_amount")
          ,$"_tmp".getItem(14).cast("float").as("tolls_amount")
          ,$"_tmp".getItem(15).cast("float").as("improvement_surcharge")
          ,$"_tmp".getItem(16).cast("float").as("total_amount")
         )
   .drop("_tmp")
)
sendingData(2)
display(streamData)

// //Write/Append streaming data to file
// streamData.writeStream.format("csv").outputMode("append").option("checkpointLocation", "/FileStore/MLStreamCheckPoint.csv").option("path", "/FileStore/StreamData").start()

// //Read data from DBFS Staging
// val Schema = (
//     new StructType()
//        .add(StructField("VendorID", StringType))
//        .add(StructField("tpep_pickup_datetime", TimestampType))
//        .add(StructField("tpep_dropoff_datetime", TimestampType))
//        .add(StructField("passenger_count", IntegerType))
//        .add(StructField("trip_distance", FloatType))
//        .add(StructField("RatecodeID", StringType))
//        .add(StructField("store_and_fwd_flag", StringType))
//        .add(StructField("PULocationID", StringType))
//        .add(StructField("DOLocationID", StringType))
//        .add(StructField("payment_type", StringType))
//        .add(StructField("fare_amount", FloatType))
//        .add(StructField("extra", FloatType))
//        .add(StructField("mta_tax", FloatType))
//        .add(StructField("tip_amount", FloatType))
//        .add(StructField("tolls_amount", FloatType))
//        .add(StructField("improvement_surcharge", FloatType))
//        .add(StructField("total_amount", FloatType))
//   )
  
// val StreamingData = (
//     sqlContext
//    .readStream
//    .option("maxEventsPerTrigger", 1)
//    .schema(Schema)
//    .csv("/FileStore/StreamData")
//   )
// sendingData(2)
// display(StreamingData)

// COMMAND ----------

// MAGIC %md ## STRUCTURED STREAMING API ##
// MAGIC 
// MAGIC Structured Streaming is integrated into Spark’s Dataset and DataFrame APIs; in most cases, you only need to add a few method calls to run a streaming computation. It also adds new operators for windowed aggregation and for setting parameters of the execution model (e.g. output modes). Streams in Structured Streaming are represented as DataFrames or Datasets with the isStreaming property set to true. You can create them using special read methods from various sources.

// COMMAND ----------

// MAGIC %md **NOTE:** Event Hubs has a small internal buffer (~256kB by default). Due to this reason, the incoming stream for successive queries might be slow or take some time to load. Please wait for a couple of minutes after firing your query to see the results appear. You can track the progress of incoming events in the Input vs Processing Rate graph by clicking on `display_query`.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Mapping, Filtering and Running Aggregations ###
// MAGIC 
// MAGIC Structured Streaming programs can use DataFrame and Dataset’s existing methods to transform data, including map, filter, select, and others. In addition, running (or infinite) aggregations, such as a count from the beginning of time, are available through the existing APIs. Following are some examples of real world use cases that use functionality of aggregations, filtering and mapping on streaming dataset.

// COMMAND ----------

// MAGIC %md  ###Find moving count of total passengers in New York City for each area###
// MAGIC 
// MAGIC The count is increases as the passenger in a particular area is arrive. Uses `groupBy` on Pickup 'LocationID', its get the unique area with the total passenger in that area, while aggregate sum of passengers in the area by using `orderBy` on sum of passengers in descending order is provide the most no. of passanger according to the area.

// COMMAND ----------

val movingCountbyLocation = (
    streamData
   .groupBy($"PULocationID")
   .agg(sum($"passenger_count").as("passenger_count"))
   .orderBy($"passenger_count".desc)
)
sendingData(3)
display(movingCountbyLocation)

// COMMAND ----------

// MAGIC %md ###Get Realtime Average fare or earning of all Cab Vendors###
// MAGIC 
// MAGIC Uses 'groupBy' on VendorID to get unique vendor and aggregate average function on total_amount to get the average amount earned by the vendors.

// COMMAND ----------

val realtimeAverageFarebyVendor = (
    streamData
   .groupBy($"VendorID")
   .agg(avg($"total_amount").as("avg_amount"))
   .orderBy($"avg_amount".desc)
)
sendingData(4)
display(realtimeAverageFarebyVendor)

// COMMAND ----------

// MAGIC %md ###Find the moving count of payments made by each payment mode.###
// MAGIC 
// MAGIC Using `groupBy` on payment_type to get unique payment_type and aggregate sum on amount with count of payment type to get the no. of payments made by each payment_type with the total amount which get paid through the particular payment type.

// COMMAND ----------

val movingCountByPayment = (
    streamData
   .groupBy($"payment_type")
   .agg(sum($"total_amount"),count($"payment_type"))
)
sendingData(5)
display(movingCountByPayment)

// COMMAND ----------

// MAGIC %md ### Cumulative sum of Distance covered and Fare earned by all Cabs of both the Vendors###
// MAGIC 
// MAGIC Uses groupBy on VendorID and date to get the unique vendorID with the each date while aggregate sum of passenger_count, trip_distance and total_amount and ordered by the date to get the highest gained vendor on a particular date, Considering the total passanger, trip distance and total amount on that particular date. 

// COMMAND ----------

val sumDistanceAndFareByVendor = (    
    streamData
   .groupBy($"VendorID",to_date($"tpep_pickup_datetime").alias("date"))
   .agg(sum($"passenger_count").as("total_passenger"),sum($"trip_distance").as("total_distance"),sum($"total_amount").as("total_amount"))
   .orderBy($"date")
  )
sendingData(6)
display(sumDistanceAndFareByVendor)

// COMMAND ----------

// MAGIC %md ###Find the busiest route : Get  highest Realtime passenger count  between all Pickups and DropOff Locations###
// MAGIC 
// MAGIC Grouped the pickup location, Drop location and date to get the unique route of between two location with aggregating count of total passanger on the route while ordering the date and passanger count in desending order to get the latest date and most no of passanger on that particular route. 

// COMMAND ----------

val busiestRoutesStream = (
    streamData
   .groupBy($"PULocationID", $"DOLocationID",to_date($"tpep_pickup_datetime").alias("date"))
   .agg(count($"passenger_count").as("passenger_count"))
   //.orderBy($"date".desc,$"passenger_count".desc)
)
sendingData(7)
display(busiestRoutesStream)                                                               

// COMMAND ----------

// MAGIC %md  ## Windowed Aggregations on Event Time ##
// MAGIC 
// MAGIC Streaming applications often need to compute data on various types of windows, including sliding windows, which overlap with each other (e.g. a 1-hour window that advances every 5 minutes), and tumbling windows, which do not (e.g. just every hour). In Structured Streaming, windowing is simply represented as a group-by. Each input event can be mapped to one or more windows, and simply results in updating one or more result table rows. 
// MAGIC #####Window functions :#####
// MAGIC Window aggregate functions are functions that perform a calculation over a group of records called window that are in some relation to the current record (i.e. can be in the same partition or frame as the current row).In other words, when executed, a window function computes a value for each and every row in a window.

// COMMAND ----------

// MAGIC %md ###USE CASE: Get top pickup locations with most passengers in last 5/10 seconds.###
// MAGIC 
// MAGIC Uses groupBy on Pickup LocationID and (window function) on pickup datetime to get the time window of 10 seconds according to the location and aggregate sum of passenger in that particular window of time with location, order the window and passanger count to get the top pickup location with most passenger in last 10 seconds of time duration. 

// COMMAND ----------

val topPickupLocationsByWindow = (
    streamData
   .groupBy($"PULocationID",window($"tpep_pickup_datetime", "10 second", "10 second"))
   .agg(sum($"passenger_count").as("passenger_count"))
   .orderBy($"window".desc,$"passenger_count".desc)
   .select("PULocationID","window.start","window.end","passenger_count")
)
sendingData(8)
display(topPickupLocationsByWindow)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Joining Streams with Static Data##
// MAGIC 
// MAGIC Because Structured Streaming simply uses the DataFrame API, it is straightforward to join a stream against a static DataFrame. Moreover, the static DataFrame could itself be computed using a Spark query, allowing us to mix batch and streaming computations.
// MAGIC 
// MAGIC We will perform the join between stream and batch data by getting a Lookup table that has details for each Location ID. Then we can perform join operation between the lookup and our data stream to know the details for the Pickup and Drop Location for each incoming stream.
// MAGIC 
// MAGIC ###Lookup-Table###
// MAGIC Lookup data is the data of describing the LocationID containing the Borough, Zone and service_zone columns of the particular LocationID. By using this data we getting the all information of the Locations which containing only the indexed no. in the tripdata file. 

// COMMAND ----------

//LOAD LOOKUP TABLE
val lookUpPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/taxi _zone_lookup.csv"
val lookUp = spark.read.option("header", true).csv(lookUpPath)
display(lookUp)

// COMMAND ----------

// MAGIC %md ###Perform join operation between batch and stream data###
// MAGIC Structured Streaming have the functionality to join the stream data with batch data, by joinig the lookup data(batch data) with the trip data(stream data) is the showcase of the structured streaming join functionality with the batch data.
// MAGIC This will get us the Borough, Zone and Service Zone for each incoming event.

// COMMAND ----------

sendingData(8)
val joinData = (
    lookUp
   .select($"LocationID" as "PULocationID"
          ,$"Borough" as "PUBorough"
          ,$"Zone" as "PUZone"
          ,$"service_zone" as "PUService_Zone"
         )
   .join(streamData,"PULocationID")
 ) 
val joinedData = (
    lookUp
   .select($"LocationID" as "DOLocationID"
          ,$"Borough" as "DOBorough"
          ,$"Zone" as "DOZone"
          ,$"service_zone" as "DOService_Zone" 
         )   
   .join(joinData,"DOLocationID")
  )
display(joinedData)