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
- Create it in East US 2 - same region, same resource group

### 2.0.5. Provision an Azure Databricks workspace
- Select "Standard" in the pricing tier
- Give it a prefix of adfv2ws followed by any suffix for uniqueness<br>
- Ensure you pick the right resource group and region - East US 2<br><br>
Leave all other options as defaults

### 2.0.6. Provision a Databricks cluster in your workspace
Select:
- cluster mode - standard
- databricks runtime - 5.1
- enable auto-scaling - uncheck
- terminate after - 30 minutes of inactivity
- worker type - ds3v2
- worker count - 3<br><br>
Leave all other entries with default values and click create.

### 2.0.7. Download the Databricks DBC
The DBC is at -


### 2.0.8. Import the DBC into your Databricks workspace
Follow the instructor and persist to you use home directory in the Databricks workspace

# 3. About the NYC taxi public dataset 

This dataset contains detailed trip-level data on New York City Taxi trips. It was collected from both drivers inputs as well as the GPS coordinates of individual cabs. 

**The data is in a CSV format and has the following fields:**
* tripID: a unique identifier for each trip
* VendorID: a code indicating the provider associated with the trip record
* tpep_pickup_datetime: date and time when the meter was engaged
* tpep_dropoff_datetime: date and time when the meter was disengaged
* passenger_count: the number of passengers in the vehicle (driver entered value)
* trip_distance: The elapsed trip distance in miles reported by the taximeter
* RatecodeID: The final rate code in effect at the end of the trip -1= Standard rate -2=JFK -3=Newark -4=Nassau or Westchester -5=Negotiated fare -6=Group ride
* store_and_fwd_flag: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor because the vehicle did not have a connection to the server - Y=store and forward; N=not a store and forward trip
* PULocationID: TLC Taxi Zone in which the taximeter was engaged
* DOLocationID: TLC Taxi Zone in which the taximeter was disengaged
* payment_type: A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip
* fare_amount: The time-and-distance fare calculated by the meter.
* extra: Miscellaneous extras and surcharges
* mta_tax: $0.50 MTA tax that is automatically triggered based on the metered rate in use
* tip_amount: Tip amount –This field is automatically populated for credit card tips. Cash tips are not included
* tolls_amount:Total amount of all tolls paid in trip
* improvement_surcharge: $0.30 improvement surcharge assessed trips at the flag drop.
* total_amount: The total amount charged to passengers. Does not include cash tips.

# 4. Databricks notebooks - workflow - review


# 5. Databricks workflow - test run


# 6. ADFv2 scheduling/orchestration 
