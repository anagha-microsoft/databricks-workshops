# Databricks workshop

This is a multi-part workshop in Spark on Databricks. It covers basics of working with Azure Data Services from Spark on Databricks, followed by an end-to-end data engineering workshop with the NYC Taxi dataset.<br>
<br>
## Target audience:
Architects and Data engineers<br>

##  Pre-requisite knowledge:
Prior knowledge of Spark, will be beneficial<br>

## Azure pre-requisites:
A subscription with at least $200 credit for a continuous 10-14 hours of usage.<br>

## 1.  Module 01-Primer (Scala):
This module covers integrating with Azure Data Services from Spark on Databricks.<br>
At the end of this module, you will know how to provision, configure, and integrate from Spark with-<br>
1.  Azure storage - blob storage and ADLS gen2; Includes Databricks Delta as well<br>
2.  Azure Event Hub - publish and subscribe, with structured streaming; Includes Databricks Delta<br>
3.  HDInsight Kafka - publish and subscribe, with structured streaming<br>
4.  Azure SQL database - read/write primer in batch and structured streaming<br>
5.  Azure SQL datawarehouse - read/write primer in batch and structured streaming<br>
6.  Azure Cosmos DB - read/write primer in batch and structured streaming; Includes structured streaming aggregation computation<br>
7.  PowerBI - basics of integration and visualization<br><br>

The Chicago crimes dataset is leveraged in the labs.

## 2.  Module 02-Data engineering workshop (Scala):
This is a *batch focused* module and covers-<br>
1.  Organizing data in the file system (ADLS gen2) - best practices, directory layout, mount storage etc<br>
2.  Load transaction data, reference data - persist to Parquet format, create external tables for the **RAW** zone<br>
3.  Transform data - cleanse, de-duplicate, apply business logic and derive/transform and persist to Parquet; Map disparate schemas to a canonical data model; Create external tables in the **CURATED** zone<br>
4.  Create denormalized, materialized views (tables) in Delta/Parquet and persist to **CONSUMPTION** zone of storage; The layer will give the best performance from a storage and query perspective.  Create external tables on the datasets<br>
5.  Generate canned reports & visualization, and persist to Parquet to the **DISTRIBUTION** zone of storage<br>
6.  Integrate reports generated to a reporting datamart RDBMS<br>
7.  Create a batch job to automate report generation in Spark and and integration with reporting datamart for BI<br>

## Next
[Provisioning guide](docs/1-provisioning-guide/ProvisioningGuide.md)<br>
[Lab data copy guide](docs/3-data-copy-guide/README.md)<br>
[Lab guide](docs/2-lab-guide/README.md)
