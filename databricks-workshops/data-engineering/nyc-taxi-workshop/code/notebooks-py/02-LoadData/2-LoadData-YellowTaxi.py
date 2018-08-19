# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC We run the common functions notebook so we can reuse capability defined there, and then...<BR>
# MAGIC 1) Load yellow taxi data in staging directory to raw data directory, and save as parquet<BR> 
# MAGIC 2) Create external unmanaged Hive tables<BR>
# MAGIC 3) Create statistics for tables                          

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType

# COMMAND ----------

#Source, destination directories
srcDataDirRoot = "/mnt/<userid>/data/nyctaxi/stagingDir/transactional-data/" 
destDataDirRoot = "/mnt/<userid>/data/nyctaxi/rawDir/<userid>/yellow-taxi" 

#Canonical ordered column list for yellow taxi across years to homogenize schema
canonicalTripSchemaColList = ["taxi_type","vendor_id","pickup_datetime","dropoff_datetime","store_and_fwd_flag","rate_code_id","pickup_location_id","dropoff_location_id","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count","trip_distance","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","payment_type","trip_year","trip_month"]

# COMMAND ----------

canonicalTripSchemaColList


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Execute notebook with common/reusable functions 

# COMMAND ----------

# MAGIC %run "../01-General/3-CommonFunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Define schema for source data
# MAGIC Different years have different schemas - fields added/removed

# COMMAND ----------

#Schema for data based on year and month

#2017
yellowTripSchema2017H1 = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)])

#Second half of 2016
yellowTripSchema2016H2 = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("junk1", StringType(), True),
    StructField("junk2", StringType(), True)])

#2015 and 2016 first half of the year
yellowTripSchema20152016H1 = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)])

#2009 though 2014
yellowTripSchemaPre2015 = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Some functions

# COMMAND ----------

#1) Function to determine schema for a given year and month
#Input:  Year and month
#Output: StructType for applicable schema 
#Sample call: print getSchemaStruct(2009,1)

def getTaxiSchema(tripYear, tripMonth):
  taxiSchema = None

  if(tripYear > 2008 and tripYear < 2015):
    taxiSchema = yellowTripSchemaPre2015
  elif(tripYear == 2016 and tripMonth > 6):
    taxiSchema = yellowTripSchema2016H2
  elif((tripYear == 2016 and tripMonth < 7) or (tripYear == 2015)):
    taxiSchema = yellowTripSchema20152016H1
  elif(tripYear == 2017 and tripMonth < 7):
    taxiSchema = yellowTripSchema2017H1
  
  return taxiSchema

# COMMAND ----------

#2) Function to add columns to dataframe as required to homogenize schema
#Input:  Dataframe, year and month
#Output: Dataframe with homogenized schema 
#Sample call: println(getSchemaHomogenizedDataframe(DF,2014,6))

from pyspark.sql.functions import *

def getSchemaHomogenizedDataframe(sourceDF,tripYear, tripMonth):
  if(tripYear > 2008 and tripYear < 2015):
    sourceDF = (sourceDF.withColumn("pickup_location_id", lit(0).cast("integer"))
              .withColumn("dropoff_location_id", lit(0).cast("integer"))
              .withColumn("improvement_surcharge",lit(0).cast("double"))
              .withColumn("junk1",lit(""))
              .withColumn("junk2",lit(""))
              .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
              .withColumn("taxi_type",lit("yellow"))
              .withColumn("temp_pickup_longitude", col("pickup_longitude").cast("string"))
                                      .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
              .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast("string"))
                                      .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
              .withColumn("temp_pickup_latitude", col("pickup_latitude").cast("string"))
                                      .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
              .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast("string"))
                                      .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
              .withColumn("temp_payment_type", col("payment_type").cast("string")).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type"))
  elif((tripYear == 2016 and tripMonth < 7) or (tripYear == 2015)):
    sourceDF = (sourceDF.withColumn("pickup_location_id", lit(0).cast("integer"))
              .withColumn("dropoff_location_id", lit(0).cast("integer"))
              .withColumn("junk1",lit(""))
              .withColumn("junk2",lit(""))
              .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
              .withColumn("taxi_type",lit("yellow"))
              .withColumn("temp_vendor_id", col("vendor_id").cast("string")).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id")
              .withColumn("temp_pickup_longitude", col("pickup_longitude").cast("string"))
                                      .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
              .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast("string"))
                                      .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
              .withColumn("temp_pickup_latitude", col("pickup_latitude").cast("string"))
                                      .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
              .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast("string"))
                                      .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
              .withColumn("temp_payment_type", col("payment_type").cast("string")).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type"))
  elif(tripYear == 2016 and tripMonth > 6):
    sourceDF = (sourceDF.withColumn("pickup_longitude", lit(""))
              .withColumn("pickup_latitude", lit(""))
              .withColumn("dropoff_longitude", lit(""))
              .withColumn("dropoff_latitude", lit(""))
              .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
              .withColumn("taxi_type",lit("yellow"))
              .withColumn("temp_vendor_id", col("vendor_id").cast("string")).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id")
              .withColumn("temp_payment_type", col("payment_type").cast("string")).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type"))
  elif(tripYear == 2017 and tripMonth < 7):
    sourceDF = (sourceDF.withColumn("pickup_longitude", lit(""))
              .withColumn("pickup_latitude", lit(""))
              .withColumn("dropoff_longitude", lit(""))
              .withColumn("dropoff_latitude", lit(""))
              .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
              .withColumn("taxi_type",lit("yellow"))
              .withColumn("junk1",lit(""))
              .withColumn("junk2",lit(""))
              .withColumn("temp_vendor_id", col("vendor_id").cast("string")).drop("vendor_id").withColumnRenamed("temp_vendor_id", "vendor_id")
              .withColumn("temp_payment_type", col("payment_type").cast("string")).drop("payment_type").withColumnRenamed("temp_payment_type", "payment_type"))
  else:
    sourceDF
    
  return sourceDF


# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Create Hive external table

# COMMAND ----------

# MAGIC %sql
# MAGIC use <userid>_taxi_db;
# MAGIC DROP TABLE IF EXISTS yellow_taxi_trips;
# MAGIC CREATE TABLE IF NOT EXISTS yellow_taxi_trips(
# MAGIC taxi_type STRING,
# MAGIC vendor_id STRING,
# MAGIC pickup_datetime TIMESTAMP,
# MAGIC dropoff_datetime TIMESTAMP,
# MAGIC store_and_fwd_flag STRING,
# MAGIC rate_code_id INT,
# MAGIC pickup_location_id INT,
# MAGIC dropoff_location_id INT,
# MAGIC pickup_longitude STRING,
# MAGIC pickup_latitude STRING,
# MAGIC dropoff_longitude STRING,
# MAGIC dropoff_latitude STRING,
# MAGIC passenger_count INT,
# MAGIC trip_distance DOUBLE,
# MAGIC fare_amount DOUBLE,
# MAGIC extra DOUBLE,
# MAGIC mta_tax DOUBLE,
# MAGIC tip_amount DOUBLE,
# MAGIC tolls_amount DOUBLE,
# MAGIC improvement_surcharge DOUBLE,
# MAGIC total_amount DOUBLE,
# MAGIC payment_type STRING,
# MAGIC trip_year STRING,
# MAGIC trip_month STRING)
# MAGIC USING parquet
# MAGIC partitioned by (trip_year,trip_month)
# MAGIC LOCATION '/mnt/<userid>/data/nyctaxi/rawDir/<userid>/yellow-taxi/';

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Read CSV, homogenize schema across years, save as parquet

# COMMAND ----------

#Delete any residual data from prior executions for an idempotent run
dbutils.fs.rm(destDataDirRoot,recurse=True)

# COMMAND ----------

#Process data, save as parquet
for j in range(2016,2017):
  endMonth = None
  if (j==2017):
    endMonth = 6 
  else: endMonth = 12 
  for i in range(1,endMonth+1):
    #Source path  
    srcDataFile= "{}year={}/month={:02d}/type=yellow/yellow_tripdata_{}-{:02d}.csv".format(srcDataDirRoot,j,i,j,i)
    print("Year={}; Month={}".format(j,i))
    print(srcDataFile)

    #Destination path  
    destDataDir = "{}/trip_year={}/trip_month={:02d}/".format(destDataDirRoot,j,i)

    #Source schema
    taxiSchema = getTaxiSchema(j,i)

    #Read source data
    taxiDF = (sqlContext.read.format("csv")
                    .option("header", True)
                    .schema(taxiSchema)
                    .option("delimiter",",")
                    .load(srcDataFile).cache())


    #Add additional columns to homogenize schema across years
    taxiFormattedDF = getSchemaHomogenizedDataframe(taxiDF, j, i)

    #Order all columns to align with the canonical schema for yellow taxi
    taxiCanonicalDF = taxiFormattedDF.select(*canonicalTripSchemaColList)

    #To make Hive Parquet format compatible with Spark Parquet format
    sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

    #Write parquet output, calling function to calculate number of partition files
    #taxiCanonicalDF.coalesce(calcOutputFileCountTxtToPrq(srcDataFile, 64)).write.parquet(destDataDir)  
            # <- TODO . wasn't able to access the information necessary from pyspark to do this calculation. Just taking the default
    taxiCanonicalDF.write.parquet(destDataDir, mode="overwrite")
    
    
    #Delete residual files from job operation (_SUCCESS, _start*, _committed*)
    for fileinfo in dbutils.fs.ls(destDataDir):
      if "parquet" not in fileinfo.path:
         dbutils.fs.rm(fileinfo.path)

    #Add partition for year and month
    sql("ALTER TABLE <userid>_taxi_db.yellow_taxi_trips ADD IF NOT EXISTS PARTITION (trip_year={},trip_month={:02d}) LOCATION '{}'".format(j,i,destDataDir))

    #Refresh table
    sql("REFRESH TABLE <userid>_taxi_db.yellow_taxi_trips")

#Compute statistics for table for performance
sql("ANALYZE TABLE <userid>_taxi_db.yellow_taxi_trips COMPUTE STATISTICS")


# COMMAND ----------


#TODO
tempdf = spark.read.parquet("/mnt/<userid>/data/nyctaxi/rawDir/<userid>/yellow-taxi/trip_year=2016/trip_month=01")
tempdf.rdd.getNumPartitions()
# display(tempdf.filter("passenger_count<10").groupby("passenger_count").count().sort(col("count").desc()))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from <userid>_taxi_db.yellow_taxi_trips