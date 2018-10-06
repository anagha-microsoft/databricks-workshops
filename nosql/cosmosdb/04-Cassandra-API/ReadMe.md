This module demonstrates by example how to work with the Azure Cosmos DB Cassandra API from Spark.<br>

This is a community effort to demystify working with Azure Cosmos Db Cassandra API, through self-contained samples that leverage Azure PaaS services - Databricks, HDInsight Kafka & Azure Event Hub.<br>
**Contributions welcome!**

The following functionality is covered:
1.0. About the Azure Cosmos DB Cassandra API, including provisioning and dependencies
2.1. DDL operations - keyspace and table
2.2. Create operations 
2.3. Read operations
2.4. Upsert operations
2.5. Delete operations
2.6. Aggregation operations
2.7. Table copy operation
2.8. Complex datatypes
2.9. Bulk load from blob - set of 2 notebooks - Chicago crimes dataset, 1.5 GB, 6.7M records; Download from internet, parse, curate, persist to Cosmos DB
2.10. Bulk load from Kafka in batch mode - set of 3 notebooks - covers HDInsight Kafka provisioning, producer and Spark consumer
2.11. Stream ingest from Kafka - set 4 notebooks - covers producer and Spark consumer (structured streaming and legacy streaming), includes structured stream sinking to Databricks Delta
2.12. Stream ingest from Azure Event Hub - set 4 notebooks - covers producer and Spark consumer (structured streaming and legacy streaming), includes structured stream sinking to Databricks Delta
2.13. End to end Azure IoT processing - simulate device telemetry feed to Azure IoT hub, ingest with Spark structured stream processing, parse, format, persist to a Cassandra table with clustering keys
2.14. Real-time changed data captured processing - read Cosmos DB change feed, process and persist

For production quality, due diligence is required - these are merely quick-start samples.
