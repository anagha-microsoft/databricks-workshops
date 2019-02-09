# 1.0. About
This Databricks Spark workshop covers basics of Spark structured streaming on Azure Databricks, with Azure Blob Storage for persistence, with Azure Event Hub as the streaming source, and with Delta as the persistence format. This is a one hour technical lab with a pre-provisioned emvironment for Microsoft Ready attendees.

## Public dataset:
[NYC Taxi public dataset](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)<br>
Transactional data = 18,878,949 taxi trips from 2017<br>
Reference data = 65 taxi zones<br>
We will import this data in the lab from an Azure Blob Storage container<br>

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
2.  Joining stream consumed data with a static data source in blob storage<br>
3.  Compute a few aggregates like count, average and sum<br>
4.  Compute windowed aggregates<br>
5.  Sink to Databricks Delta<br>

# 2.0. Provisioning
If you are Ready Winter 2019 lab attendee, you can skip this session as the environment will be pre-provisioned for you.<br>
For everyone else, here is provisioning guidance:
<br>
1a. Create a resource group<br>
<br>
2a. Create a storage account (gen1) in your resource group, same Azure region<br>
2b. Create blob storage containers - staging, raw, curated <br>
<br>
3a. Create an Azure Event Hub namespace in your resource group - "standard" tier with 4 throughput units, same Azure region<br>
3b. Create an Azure Event Hub instance within the namespace called nyc-aeh-topic, with 2 partitions<br>
3c. Create an Azure Event Hub  consumer group called nyc-aeh-topic-streaming-cg within nyc-aeh-topic<br>
<br>
4a. Create a Databricks workspace in your resource group, same Azure region<br>
4b. Create a Databricks cluster with 5 workers of SKU standard DS3v2<br>

The rest of the steps are detailed in the Databricks notebooks.<br>

# 3.0. What next?
Import the DBC at https://github.com/anagha-microsoft/databricks-workshops/blob/master/msready2019/dbc/streaming-delta-lab.dbc into your workspace.  The notebooks are numbered, start from the very first notebook and run through all the notebooks.

