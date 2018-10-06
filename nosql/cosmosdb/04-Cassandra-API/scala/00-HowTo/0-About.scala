// Databricks notebook source
// MAGIC %md
// MAGIC # About
// MAGIC This is a community effort to demystify working with Cosmos Db Cassandra API, through self-contained samples that leverage Azure PaaS services spanning Databricks, HDInsight Kafka & Azure Event Hub. <BR> **Contributions welcome!**<BR>
// MAGIC 
// MAGIC The following functionality is covered:<BR>
// MAGIC   1.0.  About the Azure Cosmos DB Cassandra API, including provisioning and dependencies<BR>
// MAGIC   2.1.  DDL operations - keyspace and table<BR>
// MAGIC   2.2.  Create operations <BR>
// MAGIC   2.3.  Read operations<BR>
// MAGIC   2.4.  Upsert operations<BR>
// MAGIC   2.5.  Delete operations<BR>
// MAGIC   2.6.  Aggregation operations<BR>
// MAGIC   2.7.  Table copy operation<BR>
// MAGIC   2.8.  Complex datatypes<BR>
// MAGIC   2.9.  Bulk load from blob - set of 2 notebooks - Chicago crimes dataset, 1.5 GB, 6.7M records; Download from internet, parse, curate, persist to Cosmos DB<BR>
// MAGIC   2.10. Bulk load from Kafka in batch mode - set of 3 notebooks - covers HDInsight Kafka provisioning, producer and Spark consumer<BR>
// MAGIC   2.11. Stream ingest from Kafka - set 4 notebooks - covers producer and Spark consumer (structured streaming and legacy streaming), includes structured stream sinking to Databricks Delta<BR>
// MAGIC   2.12. Stream ingest from Azure Event Hub - set 4 notebooks - covers producer and Spark consumer (structured streaming and legacy streaming), includes structured stream sinking to Databricks Delta<BR>
// MAGIC   2.13. Real-time changed data captured processing - read Cosmos DB change feed, process and persist<BR>
// MAGIC   2.14. End to end Azure IoT processing - simulate device telemetry to Azure IoT hub, ingest with Spark structured stream processing, parse, format, persist to a Cassandra table with clustering keys<BR>
// MAGIC   
// MAGIC **For production quality**, due diligence is required - these are merely quick-start samples.<BR>
// MAGIC   