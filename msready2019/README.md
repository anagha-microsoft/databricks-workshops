# About
This workshop covers basics of Spark structured streaming on Azure Databricks, with Azure Blob Storage for persistence, with Azure Event Hub as the streaming source, and with Delta as the persistence format. 

## Public dataset:
NYC Taxi public dataset; Transactional data = ; Reference data of taxi zone = <br>
[Original dataset source](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

## Lab tasks at a high level:
You will learn to:<br>
1.  Create an event hub namespace and consumer group
2.  Attach the Spark Structured Streaming connector for Azure Event Hub to your cluster<br>
3.  Stream publish to Azure Event Hub from Spark on Azure Databricks<br>
4.  Stream consume from Azure Event Hub<br>
5.  Run near-real-time (NRT) analytics on the stream<br>
6.  Sink to Databricks Delta and understand capabilities of Delta<br>

## Structured Streaming functionality covered:
You will learn to:<br>
1.  Parse and transform events in-flight in a stream from Azure Event Hub
2.  Joining stream consumed with a static data source in blob storage<br>
3.  Compute a few aggregation operations like count, average and sum<br>
4.  Compute windowed aggregations<br>
5.  Sink to Databricks Delta<br>
