# 1. About the workshop


# 2. Setup

### 2.0.1. Provision an Azure resource group in US East 2
Call it adfv2ws-rg

### 2.0.2. Provision a general purpose v2 Azure storage account in the resource group from 2.0.1
Give it a prefix of adfv2ws followed by any suffix for uniqueness

### 2.0.2. Create containers in your Azure blob storage account 
Create one container called (1) raw, one called (2) curated - with access level of private (no anonymous access)

### 2.0.3. Make a note of the storage account name and key 
Note this down - we will use this for mounting the storage account

### 2.0.4. Provision an Azure Data Factory v2
- Create it in East US 2 in the same region, same resource group

### 2.0.5. Provision an Azure Databricks workspace
- Select "Standard" in the pricing tier
- Give it a prefix of adfv2ws followed by any suffix for uniqueness<br>
- Ensure you pick the right resource group and region - East US 2
Leave all other options as defaults

### 2.0.6. Provision a Databricks cluster in your workspace
Select:
- cluster mode - standard
- databricks runtime - 5.1
- enable auto-scaling - uncheck
- terminate after - 30 minutes of inactivity
- worker type - ds3v2
- worker count - 3
Leave all other entries with default values and click create.

### 2.0.7. Download the Databricks DBC
The DBC is at -


### 2.0.8. Import the DBC into your Databricks workspace
Follow the instructor and persist to you use home directory in the Databricks workspace

# 3. Databricks notebooks - reports - review


# 4. Databricks notebooks - workflow - review


# 5. Databricks workflow - test run


# 6. ADFv2 scheduling/orchestration 
