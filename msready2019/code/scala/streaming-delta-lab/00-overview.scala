// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC ## About this Tutorial #  
// MAGIC The goal of this tutorial us to help you understand basics of - "**Apache Spark Structured Stream Processing**" with Azure Event Hub as the streaming source.<br>
// MAGIC 
// MAGIC ## Public dataset:
// MAGIC NYC Taxi public dataset; Transactional data = ; Reference data of taxi zone = <br>
// MAGIC [Original dataset source](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
// MAGIC 
// MAGIC ## Lab tasks at a high level:
// MAGIC You will learn to:<br>
// MAGIC 1.  Create an event hub namespace and consumer group
// MAGIC 2.  Attach the Spark Structured Streaming connector for Azure Event Hub to your cluster<br>
// MAGIC 3.  Stream publish to Azure Event Hub from Spark on Azure Databricks<br>
// MAGIC 4.  Stream consume from Azure Event Hub<br>
// MAGIC 5.  Run near-real-time (NRT) analytics on the stream<br>
// MAGIC 6.  Sink to Databricks Delta and understand capabilities of Delta<br>
// MAGIC 
// MAGIC ## Structured Streaming functionality covered:
// MAGIC You will learn to:<br>
// MAGIC 1.  Parse and transform events in-flight in a stream from Azure Event Hub
// MAGIC 2.  Joining stream consumed with a static data source in blob storage<br>
// MAGIC 3.  Compute a few aggregation operations like count, average and sum<br>
// MAGIC 4.  Compute windowed aggregations<br>

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Structured Streaming # 
// MAGIC 
// MAGIC Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. You can express your streaming computation the same way you would express a batch computation on static data. The Spark SQL engine will take care of running it incrementally and continuously and updating the final result as streaming data continues to arrive. You can use the Dataset/DataFrame API in Scala, Java, Python or R to express streaming aggregations, event-time windows, stream-to-batch joins, etc. The computation is executed on the same optimized Spark SQL engine. Finally, the system ensures end-to-end exactly-once fault-tolerance guarantees through checkpointing and Write Ahead Logs. In short, Structured Streaming provides fast, scalable, fault-tolerant, end-to-end exactly-once stream processing without the user having to reason about streaming.
// MAGIC 
// MAGIC ### Structured Streaming Model Details ###
// MAGIC 
// MAGIC Conceptually, Structured Streaming treats all the data arriving as an unbounded input table. Each new item in the stream is like a row appended to the input table. We won’t actually retain all the input, but our results will be equivalent to having all of it and running a batch job.
// MAGIC 
// MAGIC ![Structured Streaming Model](https://databricks.com/wp-content/uploads/2016/07/image01-1.png)
// MAGIC 
// MAGIC The developer then defines a query on this input table, as if it were a static table, to compute a final result table that will be written to an output sink. Spark automatically converts this batch-like query to a streaming execution plan. This is called incrementalization: Spark figures out what state needs to be maintained to update the result each time a record arrives. Finally, developers specify triggers to control when to update the results. Each time a trigger fires, Spark checks for new data (new row in the input table), and incrementally updates the result.

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Azure Event Hubs#
// MAGIC 
// MAGIC Azure Event Hubs is a Big Data streaming platform and event ingestion service, capable of receiving and processing millions of events per second. Event Hubs can process and store events, data, or telemetry produced by distributed software and devices. Data sent to an event hub can be transformed and stored using any real-time analytics provider or batching/storage adapters.
// MAGIC 
// MAGIC Event Hubs is used in some of the following common scenarios:
// MAGIC 
// MAGIC - Anomaly detection (fraud/outliers)
// MAGIC - Application logging
// MAGIC - Analytics pipelines, such as clickstreams
// MAGIC - Live dashboarding
// MAGIC - Archiving data
// MAGIC - Transaction processing
// MAGIC - User telemetry processing
// MAGIC - Device telemetry streaming
// MAGIC 
// MAGIC Data is valuable only when there is an easy way to process and get timely insights from data sources. Event Hubs provides a distributed stream processing platform with low latency and seamless integration, with data and analytics services inside and outside Azure to build a complete Big Data pipeline.
// MAGIC 
// MAGIC Event Hubs represents the "front door" for an event pipeline, often called an event ingestor in solution architectures. An event ingestor is a component or service that sits between event publishers and event consumers to decouple the production of an event stream from the consumption of those events. Event Hubs provides a unified streaming platform with time retention buffer, decoupling the event producers from event consumers.
// MAGIC 
// MAGIC For estabilishing connectivity between Azure Event Hubs and Databricks, refer to these [docs](https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Databricks Delta # 
// MAGIC 
// MAGIC Databricks Delta delivers a powerful transactional storage layer by harnessing the power of Apache Spark and Databricks DBFS. The core abstraction of Databricks Delta is an optimized Spark table that
// MAGIC 
// MAGIC - Stores data as Parquet files in DBFS.
// MAGIC - Maintains a transaction log that efficiently tracks changes to the table.
// MAGIC 
// MAGIC You read and write data stored in the delta format using the same familiar Apache Spark SQL batch and streaming APIs that you use to work with Hive tables and DBFS directories. With the addition of the transaction log and other enhancements, Databricks Delta offers significant benefits:
// MAGIC 
// MAGIC #####ACID transactions#####
// MAGIC - Multiple writers can simultaneously modify a dataset and see consistent views.
// MAGIC - Writers can modify a dataset without interfering with jobs reading the dataset.
// MAGIC 
// MAGIC #####Fast read access#####
// MAGIC - Automatic file management organizes data into large files that can be read efficiently.
// MAGIC - Statistics enable speeding up reads by 10-100x and data skipping avoids reading irrelevant information.
// MAGIC 
// MAGIC With Databricks Delta, data engineers can build reliable and fast data pipelines. Databricks Delta provides many benefits including:
// MAGIC 
// MAGIC - Faster query execution with indexing, statistics, and auto-caching support
// MAGIC - Data reliability with rich schema validation and rransactional guarantees
// MAGIC - Simplified data pipeline with flexible UPSERT support and unified Structured Streaming + batch processing on a single data source.