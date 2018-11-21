# Azure Storage - Lab instructions
In this module, we will work with Azure Blob Storage and Azure Data Lake Store Gen 2.<br>
The notebook path is: <br>

### Unit 1: Secure storage credentils with Databricks secrets
We will learn to capture storage credentials and secure credentials with databricks secrets

### Unit 2: Mount storage to the cluster
With mounting storage, you can permanently attach your Azure storage to the cluster with credentials secured one-time, and access the storage like you would - a regular file system, instead of storage URIs.

### Unit 3: File system operations
This is a foundational unit and covers working with directories and files on Databricks.

### Unit 4: Read/write primer
This is also a foundational unit and will cover what you will most commonly do as a data engineer.  Load some raw data to DBFS, read it, parse it, cleanse it, augment it, persist it in a storage efficient, query efficient persistent format, with proper physical partitioning based on access pattern, create an external table on the structured data, so we can use Spark SQL on it.<br>

We will learn to  work with dataframes, create external tables and write spark SQL query.  For this exerise, we will use the Chicago crimes dataset - 1.5 GB, 6.7 million crimes.<br>

### Unit 5: Databricks Delta primer
Also a foundational unit, for the rest of the workshop - we will use Databricks Delta that has a great value proposition.  We will learn about Delta and complete the notebook that ocvers the basics.

### Unit 6: ADLS Gen2 primer
In unit 3, we worked with Azure Blob Storage, in this unit, we will work with ADLS Gen2 as DBFS.
<br>
