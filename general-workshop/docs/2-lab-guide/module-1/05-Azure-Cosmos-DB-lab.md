# 05. Azure Cosmos DB - Lab instructions

In this lab module - we will learn to integrate with Azure Cosmos DB from Spark - batch and with Spark Structured Streaming.  
- In the batch lab - the source is the curated crimes dataset in DBFS, and the target is Azure Cosmos DB.<br>
- In the structured streaming lab, the source is Azure Event Hub, and the sink is Azure Cosmos DB.<br>


## A) Provisioning and configuring

### A1. Capture URI and credentials
![3-cosmos-db](../../../images/6-cosmos-db/5.png)
<br>
<hr>
<br>

![1-cosmos-db](../../../images/6-cosmos-db/1.png)
<br>
<hr>
<br>

![2-cosmos-db](../../../images/6-cosmos-db/2.png)
<br>
<hr>
<br>

### A2. Create database and collection in the account
![1-cosmos-db](../../../images/6-cosmos-db/3.png)
<br>
<hr>
<br>

![2-cosmos-db](../../../images/6-cosmos-db/4.png)
<br>
<hr>
<br>


## B) Lab

### Unit 1. Secure credentials
Refer the notebook for instructions.

### Unit 2. Read/write in batch mode 
In this unit, we will read data in DBFS and write to Azure SQL Cosmos DB.<br>

### Unit 3. Publish to Azure Event Hub
We will re-run the event publisher from the event hub module.

### Unit 4. Consume from Azure Event Hub, sink to Azure Cosmos DB
We will leverage structured streaming to read stream from Azure Event Hub, and sink to Azure Cosmos DB.

