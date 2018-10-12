# 1.0. About
This is a workshop showcasing an end to end Azure IoT solution including:<br> 
1.  simulated device telemetry publishing to Azure IoT hub, 
2.  device telemetry from #1, sourced to Kafka on HDInsight through KafkaConnect running on the Kafka cluster<BR> 
3.  telemetry ingestion via Spark structured streaming on Azure Databricks<BR>
4.  telemetry persistence into Azure Cosmos DB<br>
5.  reports generated in Azure Databricks, persisted to an RDBMS<br>
6.  Live device stats dashboard<br>
  
Th workshop solution leverages multiple Azure services - <BR>
  - Azure IoT Hub (Scalable IoT PaaS) <BR>
  - Azure HDInsight Kafka (Scalable streaming pub-sub) - Hortonworks Kafka PaaS <BR>
  - Azure Cosmos DB (NoSQL PaaS)  - for device registry and streaming device telemetry persistent store <BR>
  - Azure Databricks (Spark PaaS) - for distributed processing <BR>
  - Azure SQL Database (SQL Server PaaS) - for reporting serve layer <BR>
  - Power BI (BI SaaS) <BR>
  
For device telemetry simulation, the workshop leverages, the "Telemetry Simulation" accelerator available at https://www.azureiotsolutions.com/Accelerators#dashboard
  
# 2.0. Get started
### 2.0.1. Provision Azure resources
[How and what to provision](ProvisioningAndConfiguration.md)

### 2.0.2. Import the workshop dbc


### 2.0.3. Start the device telemetry simulator


### 2.0.4. Start the KafkaConnect instance for Azure IoT Hub


### 2.0.5. Mount blob storage

### 2.0.6. Stream ingest from Kafka to Cosmos DB - OLTP store

### 2.0.7. Stream ingest from Kafka to Databricks Delta - Analytics store

### 2.0.8. Run reports on telemetry, persist to Azure SQL Database

### 2.0.9. Visualize in PowerBI
