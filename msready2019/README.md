# 1.0. About
This Databricks Spark workshop covers basics of Spark structured streaming on Azure Databricks, with Azure Blob Storage for persistence, with Azure Event Hub as the streaming source, and with Delta as the persistence format. 

## Public dataset:
[NYC Taxi public dataset](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)<br>
Transactional data = 18,878,949 taxi trips from 2017<br>
Reference data = 65 taxi zones<br>

## Lab tasks at a high level:
You will learn to:<br>
1.  Create an event hub instance and consumer group
2.  Attach the Spark Structured Streaming connector for Azure Event Hub to your cluster<br>
3.  Stream publish to Azure Event Hub<br>
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

# 2.0. Provisioning
If you are Ready Winter 2019 lab attendee, you can skip this session as the environment will be pre-provisioned for you.<br>
For everyone else, here is provisioning guidance:<br>
1. Create a resource group
2. Create a storage account (gen1) in your resource group, same Azure region; You will need to create containers - this is detailed in the notebooks.
3. Create an Azure Event Hub namespace in your resource group - standard tier with 4 throughput units, same Azure region
4. Create a Databricks workspace in your resource group, same Azure region
5. Create a Databricks cluster with 4 workers of SKU standard DS3v2

# 3.0. What next?
Import the DBC at https://github.com/anagha-microsoft/databricks-workshops/blob/master/msready2019/dbc/streaming-delta-lab.dbc into your workspace.  The notebooks are numbered, start from the very first notebook and run through all the notebooks.

<br>The Delta lab is a work in progress.
