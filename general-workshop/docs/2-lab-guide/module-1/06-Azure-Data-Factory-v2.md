# 05. Azure Cosmos DB - Lab instructions

In this lab module - we will learn to integrate with Azure Cosmos DB from Spark - batch and with Spark Structured Streaming.  
- **Batch:**<br>
In the batch lab - the source is the curated crimes dataset in DBFS, and the target is Azure Cosmos DB.<br>

<img src="../../../images/6-cosmos-db/12.png" width="600" height="600">
![12-cosmos-db]()

- **Streaming:**<br>
(1) In the structured streaming lab, the source is Azure Event Hub, and the sink is Azure Cosmos DB.<br>
We will leverage the core API - also referred to as SQL API/document API - the native document-oriented API of Azure Cosmos DB.<br>
(2) We will also learn to do streaming aggregations<br>

![13-cosmos-db](../../../images/6-cosmos-db/13.png)


## A) Provisioning and configuring
