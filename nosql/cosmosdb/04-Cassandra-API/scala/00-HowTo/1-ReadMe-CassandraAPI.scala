// Databricks notebook source
// MAGIC %md
// MAGIC # Azure Cosmos DB Cassandra API
// MAGIC 
// MAGIC The Cassandra API allows you to interact with Azure Cosmos DB using Apache Cassandra constructs/codebase. Azure Cosmos DB is a managed service, therefore, not all functionality available in Cassandra is applicable.  E.g. Azure Cosmos DB has its own replication,indexing etc that is not overridable.<br>
// MAGIC 
// MAGIC This module covers:<br>
// MAGIC 1) Provisioning Azure Cosmos DB - Cassandra API<br>
// MAGIC 2) Azure Cosmos DB configuration needed for Spark integration<br>
// MAGIC 3) Attaching Spark-Cassandra connector library to the cluster<br>
// MAGIC 4) Attaching Azure Cosmos DB Spark-Cassandra specific library<br>
// MAGIC 5) Cluster-wide configuration of Azure Cosmos DB instance details<br>
// MAGIC 6) Important links<br>
// MAGIC 7) Installing cqlsh for DDL operations<br>
// MAGIC 8) Need support?

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0. Provision Azure Cosmos DB Cassandra API instance
// MAGIC Details can be found here-<br>
// MAGIC https://docs.microsoft.com/en-us/azure/cosmos-db/create-cassandra-dotnet#create-a-database-account

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Azure Cosmos DB instance related configuration needed for Spark integration
// MAGIC The following lists configuration key-value pairs:<br>
// MAGIC 1) spark.cassandra.connection.host: YOUR_COSMOSDB_ACCOUNT_NAME.cassandra.cosmosdb.azure.com<br>
// MAGIC 2) spark.cassandra.connection.port: 10350<br>
// MAGIC 3) spark.cassandra.connection.ssl.enabled: true<br>
// MAGIC 4) spark.cassandra.auth.username: YOUR_COSMOSDB_ACCOUNT_NAME<br>
// MAGIC 5) spark.cassandra.auth.password: YOUR_COSMOSDB_KEY (you can get from portal)<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.0. Attaching Spark-Cassandra connector library to the cluster
// MAGIC To work with Azure Cosmos DB Cassandra API, we have to attach the Datastax Cassandra connector the cluster.<br>
// MAGIC 1.  Review the Databricks runtime version - to see the associated Spark version and find associated maven coordinates for the compatible Datastax Cassandra Spark connector-<br>
// MAGIC https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
// MAGIC 2.  Attach the connector to the cluster<br>
// MAGIC https://docs.databricks.com/user-guide/libraries.html<br>
// MAGIC Refer to the section - "Upload a Maven package or Spark package" and follow the steps to attach the connector library to the cluster.<br>
// MAGIC An example maven coordinate for Databricks Runtime 4.3, with Spark 2.3.1 and Scala 2.11 is spark-cassandra-connector_2.11-2.3.1

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.0. Attaching Azure Cosmos DB-Cassandra API specific library to the cluster
// MAGIC We need a custom connection factory as this is the only way to configure a retry policy on the connector.<br>
// MAGIC As completed in 3.0., add the following maven coordinates to attach the jar to the cluster<br>
// MAGIC Download: https://search.maven.org/artifact/com.microsoft.azure.cosmosdb/azure-cosmos-cassandra-spark-helper/1.0.0/jar<br>
// MAGIC Coordinates: com.microsoft.azure.cosmosdb:azure-cosmos-cassandra-spark-helper:1.0.0<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5.0. Cluster-wide configuration of Azure Cosmos DB instance details
// MAGIC With Spark 2.x, the entry point is Spark session.<br>
// MAGIC With the Datastax Spark connector, the dataframe API can read configuration off of that defined at a spark session level within notebook.<br>
// MAGIC NOTE: For the RDD API though, we need to use the Spark context which is initialized at cluster start-up.  We therefore need to add the details in section 2.0 at a cluster level.<br>
// MAGIC 
// MAGIC The following are options for ways to set up cluster wide conf:<br>
// MAGIC 1) Paste conf into cluster spark conf section of the Databricks UI - cluster:<br>
// MAGIC Click on the clusters icon, click on the cluster, click on edit and enter into the Spak conf space, space separated key value pairs of the details from 2.0, without quotes.  <br>
// MAGIC https://docs.databricks.com/user-guide/clusters/spark-config.html
// MAGIC <br>
// MAGIC E.g.<br>
// MAGIC ```
// MAGIC spark.cassandra.connection.ssl.enabled true
// MAGIC spark.cassandra.auth.username YOUR_COSMOSDB_ACCOUNT_NAME
// MAGIC spark.cassandra.auth.password YOUR_COSMOSDB_KEY
// MAGIC spark.cassandra.connection.port 10350
// MAGIC spark.cassandra.connection.host YOUR_COSMOSDB_ACCOUNT_NAME.cassandra.cosmosdb.azure.com
// MAGIC ```
// MAGIC ....<br>
// MAGIC Pros: Easy to setup<br>
// MAGIC Cons: Credentials exposed in clear text<br>
// MAGIC Secrets support is in the roadmap
// MAGIC <br><br>
// MAGIC 2) Use Init scripts:<br>
// MAGIC https://docs.azuredatabricks.net/user-guide/clusters/init-scripts.html#init-script-types<br>
// MAGIC Pros: Can be access controlled; Recommended method
// MAGIC 
// MAGIC **TODO**: Anagha to create example
// MAGIC <br><br>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6.0. Important links
// MAGIC 1) Link to Spark Cassandra tuning specs<br>
// MAGIC https://github.com/Azure-Samples/azure-cosmos-db-cassandra-api-spark-connector-sample<br>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.0. Using cqlsh with Azure Cosmos DB Cassandra API
// MAGIC If you are particular about using cqlsh, you can install a pseudo-distrubuted Cassandra instance on your machine, and launch cqlsh against Azure Cosmos DB-Cassandra API instance.<br><br>
// MAGIC **1) Installing - cqlsh:**
// MAGIC - Installation on Mac<br>
// MAGIC <code>"sudo brew install cassandra"</code> and you are done!
// MAGIC 
// MAGIC - Start Cassandra<br>
// MAGIC <code>bin/cassandra</code>
// MAGIC 
// MAGIC - Check status<br>
// MAGIC bin/nodetool status
// MAGIC 
// MAGIC - Launch cqlsh<br>
// MAGIC <code>bin/cqlsh</code>
// MAGIC 
// MAGIC <br>
// MAGIC **2) Launching cqlsh against Azure Cosmos DB Cassandra API:**
// MAGIC ```
// MAGIC cd bin<br>
// MAGIC export SSL_VERSION=TLSv1_2<br>
// MAGIC export SSL_VALIDATE=false<br>
// MAGIC python cqlsh.py YOUR-COSMOSDB-ACCOUNT-NAME.cassandra.cosmosdb.windows-ppe.net  10350 -u YOUR-COSMOSDB-ACCOUNT-NAME -p YOUR-COSMOSDB-ACCOUNT-KEY --ssl
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.0. Need support?
// MAGIC For any issues - please write to askcosmosdb@microsoft.com<br><br>