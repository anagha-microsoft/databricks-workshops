# 05. Azure Cosmos DB - Lab instructions

In this lab module - we will learn to integrate with Azure Cosmos DB from Spark - batch and with Spark Structured Streaming.  
- In the batch lab - the source is the curated crimes dataset in DBFS, and the target is Azure Cosmos DB.<br>
- In the structured streaming lab, the source is Azure Event Hub, and the sink is Azure Cosmos DB.<br>
We will leverage the core API - also referred to as SQL API/document API - the native document-oriented API of Azure Cosmos DB.


## A) Provisioning and configuring

### A1. Capture URI and credentials
![7-cosmos-db](../../../images/6-cosmos-db/7.png)
<br>
<hr>
<br>

![8-cosmos-db](../../../images/6-cosmos-db/8.png)
<br>
<hr>
<br>

![9-cosmos-db](../../../images/6-cosmos-db/9.png)
<br>
<hr>
<br>




### A2. Create database and collection in the account
![10-cosmos-db](../../../images/6-cosmos-db/10.png)
<br>
<hr>
<br>

![11-cosmos-db](../../../images/6-cosmos-db/11.png)
<br>
<hr>
<br>

### A3. Attach the Azure Cosmos DB Spark connector uber jar to the cluster
Go to https://search.maven.org/search?q=a:azure-cosmosdb-spark_2.3.0_2.11 or the link to the latest version of the jar as detailed [here](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/cosmosdb-connector.html).  Download the jar.<br>

Steps for attaching to the cluster:<br>

![1-cosmos-db](../../../images/6-cosmos-db/1.png)
<br>
<hr>
<br>

Click on "Keys" in the left navigation panel.
![2-cosmos-db](../../../images/6-cosmos-db/2.png)
<br>
<hr>
<br>

![3-cosmos-db](../../../images/6-cosmos-db/3.png)
<br>
<hr>
<br>

![4-cosmos-db](../../../images/6-cosmos-db/4.png)
<br>
<hr>
<br>

![5-cosmos-db](../../../images/6-cosmos-db/5.png)
<br>
<hr>
<br>

![6-cosmos-db](../../../images/6-cosmos-db/6.png)
<br>
<hr>
<br>

## B) Lab

### Unit 1. Secure credentials
Refer the notebook for instructions.

### Unit 2. Publish to Azure Event Hub
We will re-run the event publisher from the event hub module.

### Unit 2. Read/write in batch mode 
In this unit, we will read data in DBFS and write to Azure SQL Cosmos DB.<br>

### Unit 3. Publish to Azure Event Hub
We will re-run the event publisher from the event hub module.

### Unit 4. Consume from Azure Event Hub, sink to Azure Cosmos DB
We will leverage structured streaming to read stream from Azure Event Hub, and sink to Azure Cosmos DB.

