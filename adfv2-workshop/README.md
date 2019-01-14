# 1. About the workshop


# 2. Setup

### 2.0.1. Provision an Azure resource group in US East 2
Call it adfv2ws-rg

### 2.0.2. Provision a general purpose v2 Azure storage account in the resource group from 2.0.1
Give it a prefix of adfv2ws followed by any suffix for uniqueness

### 2.0.2. Create containers in your Azure blob storage account 
Create one container called raw, one called curated - with access level of private (no anonymous access)

### 2.0.3. Make a note of the storage account name and key 
Note this down - we will use this for mounting the storage account

### 2.0.4. Provision an Azure Databricks workspace
- Give it a prefix of adfv2ws followed by any suffix for uniqueness<br>
- Select "Standard" in the pricing tier
Leave all other options as defaults

2.0.4. Provision an Azure Data Factory v2

2.0.5. Download the Databricks DBC

2.0.6. Import the DBC into your cluster

# 3. Databricks notebooks - reports - review


# 4. Databricks notebooks - workflow - review


# 5. Databricks workflow - test run


# 6. ADFv2 scheduling/orchestration 
