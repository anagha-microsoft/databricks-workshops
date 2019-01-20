// Databricks notebook source
// MAGIC %md
// MAGIC # Key vault backed secrets in Azure Databricks
// MAGIC 
// MAGIC Secrets allow you to secure your credentials, and reference in your code instead of hard-code.  Databricks automatically redacts secrets from being displayed in the notebook as cleartext.<BR>
// MAGIC   
// MAGIC **Pre-requisite**: You should have created an Azure Key Vault and completed the ADFv2 primer<br><br>
// MAGIC In this notebook, we will -<br>
// MAGIC   1.  Provision Azure Key Vault
// MAGIC   2.  Create your secret scope
// MAGIC   3.  Test using Azure Key Vault secrets

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Create a secret scope in Azure Key Vault

// COMMAND ----------

// MAGIC %md
// MAGIC 1.  Go to https://<your_azure_databricks_url>#secrets/createScope (for example, https://eastus2.azuredatabricks.net#secrets/createScope).<br>
// MAGIC 2.  Create a scope
// MAGIC     - Enter scope name (gws-akv-sqldw)
// MAGIC     - Select "creator" for "Manage principal"
// MAGIC     - Enter DNS name of the Key Vault you created - you will find it in the portal, overview as DNS name
// MAGIC     - Enter the resource ID of the Key Vault you created - you will find it in the portal, in the properties page as resource ID

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Secure your blob storage credentials

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.1. Create a secrets scope for your storage account
// MAGIC You can name it per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope gws-blob-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.0.2. Set up your storage account key within the secret scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-blob-storage --key storage-acct-key```

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

// MAGIC %md ```databricks secrets list --scope gws-blob-storage```

// COMMAND ----------

// MAGIC %md You should see the secret created.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Secure your Azure Data Lake Store Gen2 (ADLSGen2) storage credentials

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.1. Create a secrets scope for your ADLSGen2 storage
// MAGIC You can name the scope per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope gws-adlsgen2-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.0.2. Set up your ADLSGen2 credentials within the secret scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen2-storage --key storage-acct-key```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen2-storage --key client-id```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen2-storage --key client-secret```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen2-storage --key tenant-id```

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

// MAGIC %md ```databricks secrets list --scope gws-adlsgen2-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4. Secure your Azure Data Lake Store Gen1 (ADLSGen1) storage credentials

// COMMAND ----------

// MAGIC %md
// MAGIC Follow the provisionin documentation to ensure dependencies are met:<br>
// MAGIC 1.  Create a service principal, and an access token<br>
// MAGIC 2.  Create a directory in ADLS Gen 1<br>
// MAGIC 3.  Grant the service principal full access - top as well as child entities to the directory<br>
// MAGIC 4.  Capture the following information we will need for mounting:<br>
// MAGIC   - SPN Application ID - we will use this for client ID
// MAGIC   - Application Access token
// MAGIC   - AAD Tenant ID

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.0.1. Create a secrets scope for your ADLSGen1 storage
// MAGIC You can name the scope per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope gws-adlsgen1-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.0.2. Set up your ADLSGen1 credentials within the secret scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen1-storage --key access-token```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen1-storage --key client-id```

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope gws-adlsgen1-storage --key tenant-id```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.0.3. List your secret scopes

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets list-scopes```

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.0.4. List the secrets within the scope created

// COMMAND ----------

// MAGIC %md ```databricks secrets list --scope gws-adlsgen1-storage```