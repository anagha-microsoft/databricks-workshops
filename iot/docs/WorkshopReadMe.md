# Working through the workshop content

## Module 1. Provisioning 
You will first need to provision all the required Azure resources upfront and complete all the specified configuration.

## Module 2. Setup 
In this section you need to complete the setup section of the Databricks notebooks.<br>

#### 2.1.  Mount blob storage

We will mount blob storage as a Databricks file system, as a one-time activity so we can seamlessly access it as a file system , and without having to provide credentials each time we access it.

**Execute the notebook 00-Setup/1-MountBlobStorage.scala** in your Databricks cluster.<br>
Be sure to replace the name of the storage account at the beginning of the notebook with your storage account name.<br><br>

Typically, we organize data in big data solutions, into multiple directories based on purpose, retention policies, and need for security.<br>
- **MasterData** - master data; Fairly static, may or may not have history requirements; optimized for fast access <br>
- **ReferenceData** - reference data; Not as static as master data; May or may not have history requirements; optimized for fast access <br>
- **Staging** - landing zone for data to be processed, transient data with short retention<br>
- **Raw** - raw data from source, stored permanently, and in full fidelity exactly as received from source, may or may not be optimized for storage and query processing.  Typically not accessible for querying other than by the application ID processing the data in an automated fashion.<br>
- **Curated** - Curated data may be all or a subset of raw data, with a specific business purpose.  It is cleansed, deduplicated, transformed, merged, augmented (with reference and master data), stored, potentially indefinitely or with a specific retention policy and access policy.  Due diligence applied when it comes to physical storage partitioning scheme for query performance, persistence format for storage and query performance optimization - e.g. Parquet for analytical/all workloads, avro for row-level.  Compression for storage optimization. This layer may be exposed for exploratory aalytics to data product designers.<br>
- **Consumption** - Materialized views, reports optimized for querying and purpose-built for specific consumers.<br><br>

We will create only a part of these in the workshop.

## Module 3. Structured Stream Processing - Device current state capture
![CurrentStateStore](../images/CurrentState.png)

In this module, we will run the notebook, **01-StreamIngest/01a-Stream-SinkTo-CosmosDB.scala**, to execute the flow described in the diagram above.  We will read events streaming into Kafka from devices, routed through Azure IoT Hub, and persist to Azure Cosmos DB.  We will persist in upsert mode, only the latest telemetry - to capture, just the current state.  In a separate notebook, we will cover persisting all telemetry.

In production, you would run this notebook, in a separate cluster, ensuring all services in the pipeline have adequate resources.

## Module 4. Structured Stream Processing - Device telemetry history capture





