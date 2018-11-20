
# Provisioning Guide

For the workshop, we will need to provision multiple resources/services.  Many of these are for the primer only as labeled below.  The following is a step-by-step provisioning guide.<br>
1.   Azure Resource group<br>
2.   Azure Virtual network<br>
3.   Azure Databricks<br>
4.   Azure Blob Storage (for the primer only)<br>
5.   Azure Data Lake Storage Gen2<br>
6.   Azure Event Hub (for the primer only)<br>
7.   Azure HDInsight Kafka (for the primer only)<br>
8.   Azure SQL Database<br>
9.   Azure SQL Datawarehouse (for the primer only)<br>
10.   Azure Cosmos DB (for the primer only)<br>
11.  Azure Key Vault (for the primer only)<br>

## 1. Provision a resource group
We will create a resource group into which we will provision all other Azure resources.

## 2.  Provision a virtual network
We will an Azure Vnet in #2. 

## 3.  Provision Azure Databricks
We will provision Azure Databricks in the Vnet we created in #2.  We will then peer the Dataricks service provisioned Vnet with the Vnet from #2 for access.  We will discuss Vnet injection.

## 4.  Provision a blob storage account
We will provision an Azure Blob Storage account.  

## 5.  Provision Azure Data Lake Store Gen2
We will provision an Azure Lake Store Gen 2 account.  

## 6.  Provision Azure Event Hub
We will provision Azure Event Hub, and a consumer group.  We will set up SAS poliies for access, and capture the credentials required for access from Spark. 

## 7.  Provision Azure HDInsight Kafka
We will provision a Kafka cluster in the Vnet from #2.  We will enable Kafka to advertise private IPs, and configure it to listen on all network interfaces. We will then create a Kafka topic.  Finally - we will capture the Kafka broker private IP addresses for use from Spark.

## 8.  Provision Azure SQL Database
We will provision a logical database server, and an Azure SQL Database within the server.  We will configure the firewall to allow our machine to access, and also enable access to the Dataricks Vnet.  We will capture credentials for access from Databricks.

## 9.  Provision Azure SQL Datawarehouse
We will provision an Azure SQL datawarehouse in the same database server created in #7.  We will create a master key as this is a one-time pre-requisite setup with Azure SQL Datawarehouse.

## 10.  Provision Azure Cosmos DB
We will provision a Cosmos DB account, database and 3 collections - one for batch load, one for stream ingest and one for streaming computations.

## 11.  Provision Azure Key Vault
This is to give you a flavor of securing credentials in Azure Key Vault.
