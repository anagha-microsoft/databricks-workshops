## 1.0. About
This is a workshop showcasing an end to end IoT solution, powered by Azure PaaS, as referenced [below](decks/ReferenceArchitecture.pptx).<br>
![ReferenceArchitecture](images/ReferenceArchitecture.png)

The workshop includes:<br> 
1.  simulated device telemetry publishing to Azure IoT hub, 
2.  device telemetry from #1, sourced to Kafka on HDInsight through KafkaConnect<BR> 
3.  telemetry ingestion via Spark structured streaming on Azure Databricks<BR>
4.  telemetry persistence into Azure Cosmos DB<br>
5.  reports generated in Azure Databricks, persisted to an RDBMS<br>
6.  live device stats dashboard<br>
and more...<br>
  
The workshop solution leverages multiple Azure PaaS services - <BR>
  - Azure IoT Hub (Scalable IoT PaaS) <BR>
  - Azure HDInsight Kafka (Scalable streaming pub-sub) - Hortonworks Kafka PaaS <BR>
  - Azure Cosmos DB (NoSQL PaaS)  - for device registry, scalable OLTP, and time series database <BR>
  - Azure Databricks (Spark PaaS) - for distributed processing <BR>
  - Azure SQL Database (SQL Server PaaS) - for reporting serve layer <BR>
  - Power BI (BI SaaS) <BR>
  
## 2.0. Workshop
### 2.0.1. Modules 
[Module 1: Provision services, configure and start services](docs/WorkshopReadMe.md#module-1-provision-services-configure-and-start-services)<BR>
[Module 2: One time setup in Databricks - mount blob storage](docs/WorkshopReadMe.md#module-2-setup)<BR>
[Module 3: Structured Stream Processing - device current state capture into Azure Cosmos DB (nosql/OLTP store)](docs/WorkshopReadMe.md#module-3-structured-stream-processing---device-current-state-capture-into-azure-cosmos-db-nosql)<BR>
[Module 4: Structured Stream Processing - device telemetry history capture into Databricks Delta (analytics store)](docs/WorkshopReadMe.md#module-4-structured-stream-processing---device-telemetry-history-capture-into-databricks-delta)<BR>
Module 5: Stateful Structured Stream Processing - moving averges capture into Azure Cosmos DB (nosql/OLTP store)<BR>
Module 6: Batch Processing - reports persisted to Azure SQL Database (reporting mart)<BR>
Module 7: Batch Processing - time series modeling and persisting to Azure Cosmos DB and Databricks Delta<BR>
Module 8: BI and visualization - sourced from SQL Database, and over Spark against blob storage<BR>
Module 9: Real time dashboard<BR>
Module 10: Anomaly detection and alerting<BR>
Module 11: Advanced analytics- AI in IoT<BR>
<br>
  
### 2.0.2. Coming soon
Modules 5 and 7
![WIP](images/WorkInProgress.png)

 
