// Databricks notebook source
// MAGIC %md
// MAGIC # Secure your storage key with Databricks secrets
// MAGIC 
// MAGIC Secrets allow you to secure your credentials, and reference in your code instead of hard-code.  Databricks automatically redacts secrets from being displayed in the notebook as cleartext.<BR>
// MAGIC   
// MAGIC **Pre-requisite**: You should have created your Azure Blob Storage account and ADLS Gen2 account.<br><br>
// MAGIC In this notebook, we will -<br>
// MAGIC   1.  Installl Dataricks CLI
// MAGIC   2.  Secure blob storage credentials
// MAGIC   3.  Secure ADLS Gen2 storage credentials
// MAGIC   

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Install Databricks CLI on your Linux terminal

// COMMAND ----------

// MAGIC %md
// MAGIC ```pip install databricks-cli```

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Secure your blob storage credentials

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.1. Create a secrets scope for your storage account
// MAGIC You can name it per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope ws-blob-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.2. Set up your storage account key within the secret scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope ws-blob-storage --key storage-acct-key```

// COMMAND ----------

// MAGIC %md
// MAGIC This will open a file for you to enter in your storage account key.  Save and close the file and your storage secret is saved.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.3. List your secret scopes

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets list-scopes```

// COMMAND ----------

// MAGIC %md You should see the scope created.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.4. List the secrets within the scope created

// COMMAND ----------

// MAGIC %md ```databricks secrets list --scope ws-blob-storage```

// COMMAND ----------

// MAGIC %md You should see the secret created.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Secure your Azure Data Lake Store Gen2 (ADLSGen2) storage credentials

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.1. Create a secrets scope for your ADLS gen2 storage
// MAGIC You can name it per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope ws-adlsgen2-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.2. Set up your ADLSGen2 key within the secret scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope ws-adlsgen2-storage --key storage-acct-key```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.3. List your secret scopes

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets list-scopes```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.4. List the secrets within the scope created

// COMMAND ----------

// MAGIC %md ```databricks secrets list --scope ws-adlsgen2-storage```