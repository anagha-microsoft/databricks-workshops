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
[AzureIoT](docs/Provisioning-1-AzureIoT.md)


