# Databricks workshop

This is a multi-part workshop in Spark on Databricks. It covers basics of working with Azure Data Services from Spark on Databricks, followed by an end-to-end data engineering workshop with the NYC Taxi dataset.<br>
<br>
**Target audience:**<br>
Architects and Data engineers<br>

**Pre-requisite knowledge:**<br>
Prior knowledge of Spark, will be beneficial<br>

**Azure pre-requisites:**<br>
A subscription with at least $200 credit for a continuous 10-14 hours of usage.<br>

**1.  Module 01-Primer (Scala):** <br>
This module covers integrating with Azure Data Services from Spark on Databricks.<br>
At the end of this module, you will know how to provision and itegrate from Spark with-<br>
(i) Azure storage - blob storage and ADLS gen2; Includes Databricks Delta as well<br>
(ii) Azure Event Hub - publish and subscribe, with structured streaming; Includes Databricks Delta<br>
(iii) HDInsight Kafka - publish and subscribe, with structured streaming<br>
(iv) Azure SQL database - read/write primer in batch and streaming<br>
(v) Azure SQL datawarehouse - read/write primer in batch and streaming<br>
(vi) Azure Cosmos DB - read/write primer in batch and streaming; Includes streaming aggregation computation<br>
(vii) PowerBI - basics of integration and visualization<br><br>

The Chicago crimes dataset is leveraged in the labs.

**2.  Module 02-Data engineering workshop (Scala):** <br>
This is a *batch focused* module and covers-<br>
(i) Organizing data in the file system (ADLS gen2) - best practices, directory layout, mount storage etc<br>
(ii) Load transaction data, reference data - persist to Parquet format, create external tables for the RAW zone<br>
(iii) Transform data - cleanse, de-duplicate, apply business logic and derive/transform and persist to Parquet; Map disparate schemas to a canonical data model; Create external tables in the CURATED zone<br>
(iv) Create denormalized, materialized views (tables) in Delta/Parquet and persist to CONSUMPTION zone of storage; The layer will give the best performance from a storage and query perspective.  Create external tables on the datasets.<br>  
(v) Generate canned reports & visualization, and persist to Parquet to the DISTRIBUTION zone of storage;<br>
(vi) Integrate reports generated to a reporting datamart RDBMS<br>
(vii) Create a batch job to automate report generation in Spark and and integration with reporting datamart for BI<br>
