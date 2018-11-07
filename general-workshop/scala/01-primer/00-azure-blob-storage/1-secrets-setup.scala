// Databricks notebook source
// MAGIC %md
// MAGIC # Secure your storage account key with Databricks secrets
// MAGIC 
// MAGIC Secrets allow you to secure your credentials, and reference in your code instead of hard-code.  Databricks automatically redacts secrets from being displayed in the notebook as cleartext.<BR>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Install Databricks CLI on your Linux terminal

// COMMAND ----------

// MAGIC %md
// MAGIC ```pip install databricks-cli```

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Create a secrets scope for your storage account
// MAGIC In this example - we are calling it bhoomi-storage.  You can name it per your preference.

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets create-scope --scope bhoomi-storage```

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Set up your storage account key secret within the scope

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets put --scope bhoomi-storage --key storage-acct-key```

// COMMAND ----------

// MAGIC %md
// MAGIC This will open a file for you to enter in your storage account key.  Save and close the file and your storage secret is saved.

// COMMAND ----------

// MAGIC %md
// MAGIC ###  4. List your secret scopes

// COMMAND ----------

// MAGIC %md
// MAGIC ```databricks secrets list-scopes```

// COMMAND ----------

// MAGIC %md You should see the scope created.

// COMMAND ----------

// MAGIC %md
// MAGIC ###  5. List the secrets within the scope

// COMMAND ----------

// MAGIC %md ```databricks secrets list --scope bhoomi-storage```

// COMMAND ----------

// MAGIC %md You should see the secret created.