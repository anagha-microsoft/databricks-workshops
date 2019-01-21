// Databricks notebook source
// MAGIC %md 
// MAGIC #### Dataset review
// MAGIC This dataset contains detailed trip-level data on New York City Taxi trips. It was collected from both drivers inputs as well as the GPS coordinates of individual cabs. 
// MAGIC 
// MAGIC 
// MAGIC The data is in a CSV format and has the following fields:
// MAGIC 
// MAGIC 
// MAGIC * tripID: a unique identifier for each trip
// MAGIC * VendorID: a code indicating the provider associated with the trip record
// MAGIC * tpep_pickup_datetime: date and time when the meter was engaged
// MAGIC * tpep_dropoff_datetime: date and time when the meter was disengaged
// MAGIC * passenger_count: the number of passengers in the vehicle (driver entered value)
// MAGIC * trip_distance: The elapsed trip distance in miles reported by the taximeter
// MAGIC * RatecodeID: The final rate code in effect at the end of the trip -1= Standard rate -2=JFK -3=Newark -4=Nassau or Westchester -5=Negotiated fare -6=Group ride
// MAGIC * store_and_fwd_flag: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor because the vehicle did not have a connection to the server - Y=store and forward; N=not a store and forward trip
// MAGIC * PULocationID: TLC Taxi Zone in which the taximeter was engaged
// MAGIC * DOLocationID: TLC Taxi Zone in which the taximeter was disengaged
// MAGIC * payment_type: A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip
// MAGIC * fare_amount: The time-and-distance fare calculated by the meter.
// MAGIC * extra: Miscellaneous extras and surcharges
// MAGIC * mta_tax: $0.50 MTA tax that is automatically triggered based on the metered rate in use
// MAGIC * tip_amount: Tip amount –This field is automatically populated for credit card tips. Cash tips are not included
// MAGIC * tolls_amount:Total amount of all tolls paid in trip
// MAGIC * improvement_surcharge: $0.30 improvement surcharge assessed trips at the flag drop.
// MAGIC * total_amount: The total amount charged to passengers. Does not include cash tips.

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 1. Load 

// COMMAND ----------

// MAGIC %md ### 1.1. Load trip transactions

// COMMAND ----------

//1.  Source, destination directories
val srcDataAbsPathTrips = "/mnt/workshop/staging/model-related/NYC_Cab.csv" 
val destDataDirRootTrips = "/mnt/workshop/raw/nyctaxi/model-transactions/trips/" 

// COMMAND ----------

//2.  Load from source into dataframe
val stagedTripsDF = spark.read.option("header","true").csv(srcDataAbsPathTrips)
stagedTripsDF.count
display(stagedTripsDF)

// COMMAND ----------

//3.  Typecasting columns and renaming
//All the data is of string datatype
//Lets cast it appropriately
import org.apache.spark.sql.types._
val tripsCastedRenamedTripsDF=stagedTripsDF.select($"tripID" as "trip_id",
                               $"VendorID" as "vendor_id",
                               $"tpep_pickup_datetime".cast(TimestampType) as "pickup_timestamp",
                               $"tpep_dropoff_datetime".cast(TimestampType) as "dropoff_timestamp",
                               $"passenger_count".cast(IntegerType),
                               $"trip_distance".cast(DoubleType),
                               $"RatecodeID" as "rate_code_id",
                               $"store_and_fwd_flag",
                               $"PULocationID" as "pickup_locn_id",
                               $"DOLocationID" as "dropoff_locn_id",
                               $"payment_type".cast(IntegerType))

// COMMAND ----------

//4.  Dataset review 1 - display
display(tripsCastedRenamedTripsDF)

// COMMAND ----------

//5.  Dataset review 2 - schema recap
tripsCastedRenamedTripsDF.printSchema

// COMMAND ----------

//6.  Dataset review 3 - descriptive statistics
tripsCastedRenamedTripsDF.describe().show()

// COMMAND ----------

//7.  Now that we have a decent idea, lets persist to the raw information zone as parquet - a more space and query-efficient persistence format
import org.apache.spark.sql.SaveMode
tripsCastedRenamedTripsDF.coalesce(2).write.mode(SaveMode.Overwrite).save(destDataDirRootTrips)

// COMMAND ----------

//8. Display file system to check file size - ensure you have at least 128 MB files or close
display(dbutils.fs.ls(destDataDirRootTrips))

// COMMAND ----------

//9) Delete residual files from job operation (_SUCCESS, _start*, _committed*)
import com.databricks.backend.daemon.dbutils.FileInfo
dbutils.fs.ls(destDataDirRootTrips + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC //10.  Create external hive table, compute statistics

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC USE taxi_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS model_raw_trips;
// MAGIC CREATE TABLE IF NOT EXISTS model_raw_trips
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/raw/nyctaxi/model-transactions/trips/");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC ANALYZE TABLE model_raw_trips COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %md //11. Review table data

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_db.model_raw_trips

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 1.2. Load trip fares dataset

// COMMAND ----------

//1.  Source, destination directories
val srcDataAbsPathFares = "/mnt/workshop/staging/model-related/NYC_Fares.csv" 
val destDataDirRootFares = "/mnt/workshop/raw/nyctaxi/model-transactions/fares/" 

// COMMAND ----------

//2.  Load from source into dataframe
val stagedFaresDF = spark.read.option("header","true").csv(srcDataAbsPathFares)
stagedFaresDF.count
display(stagedFaresDF)

// COMMAND ----------

//3.  Typecasting columns and renaming
//All the data is of string datatype
//Lets cast it appropriately
import org.apache.spark.sql.types._
val tripsCastedRenamedFaresDF=stagedFaresDF.select($"tripID" as "trip_id",
                               $"fare_amount".cast(FloatType),
                               $"extra".cast(FloatType),
                               $"mta_tax".cast(FloatType),
                               $"tip_amount".cast(FloatType),
                               $"tolls_amount".cast(FloatType),
                               $"improvement_surcharge".cast(FloatType),
                               $"total_amount".cast(FloatType))

// COMMAND ----------

//4.  Dataset review 1 - display
display(tripsCastedRenamedFaresDF)

// COMMAND ----------

display(tripsCastedRenamedFaresDF)

// COMMAND ----------

//5.  Dataset review 2 - schema recap
tripsCastedRenamedFaresDF.printSchema

// COMMAND ----------

//6.  Dataset review 3 - descriptive statistics
tripsCastedRenamedFaresDF.describe().show()

// COMMAND ----------

//7.  Now that we have a decent idea, lets persist to the raw information zone as parquet - a more space and query-efficient persistence format
import org.apache.spark.sql.SaveMode
tripsCastedRenamedFaresDF.coalesce(1).write.mode(SaveMode.Overwrite).save(destDataDirRootFares)

// COMMAND ----------

//8. Display file system to check file size - ensure you have at least 128 MB files or close
display(dbutils.fs.ls(destDataDirRootFares))

// COMMAND ----------

//9) Delete residual files from job operation (_SUCCESS, _start*, _committed*)
import com.databricks.backend.daemon.dbutils.FileInfo
dbutils.fs.ls(destDataDirRootFares + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC //10.  Create external hive table, compute statistics

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC USE taxi_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS model_raw_fares;
// MAGIC CREATE TABLE IF NOT EXISTS model_raw_fares
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/raw/nyctaxi/model-transactions/fares/");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC ANALYZE TABLE model_raw_trips COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %md
// MAGIC //11. Review table data

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_db.model_raw_fares

// COMMAND ----------

// MAGIC %md ### 2. Curate

// COMMAND ----------

// MAGIC %md #### 2.1. Fares | Outlier detection and handling/replacing ##
// MAGIC 
// MAGIC The line chart of the "fares" DataFrame shows several trips for which  `fare_amount` is $600-700, which is implausible in the real world. These data points are considered outliers; they are highly abnormal values. There are several ways to calculate what values constitute an outlier. In this scenario, consider a normal fare amount to be defined as between $0-100. Any amount above $100 will be considered an outlier. One way of managing outliers is replacing the aberrant values with the average value of the data points across the whole dataset. 

// COMMAND ----------

tripsCastedRenamedFaresDF.describe().show()

// COMMAND ----------

//1.  Replace fare outliers with mean of valid fares
import org.apache.spark.sql.functions._
//DF for fares - exclude fares less than 1
val faresDF = tripsCastedRenamedFaresDF.filter($"fare_amount">0)
//Filter the data to get rows without the outliers
val filteredFaresDF=faresDF.filter($"fare_amount"<=100)
//Calculate average fare for all the valid fares (only)
val mean=filteredFaresDF.agg(avg($"fare_amount")).first.getDouble(0)
//Insert average fare in place of outliers
val correctFaresDF=faresDF.select($"*", when($"fare_amount">100, mean).otherwise($"fare_amount").alias("fare"))
                          .drop($"fare_amount").withColumnRenamed("fare","fare_amount")

// COMMAND ----------

// MAGIC   %md  Since the `fare_amount` has been modified for several trips, all fields that are dependent on `fare_amount` need to be recalculated. In this scenario, `total_amount` is the sum of the base `fare_amount` and various other surcharges(tolls, tips etc.)

// COMMAND ----------

//2.  Calculate and augment dataset with a new column called total as a sum of all teh ride related monetary attributes
val cleansedDF=correctFaresDF.withColumn("total",$"fare_amount"+$"extra"+$"mta_tax"+$"tip_amount"+$"tolls_amount"+$"tip_amount"+$"improvement_surcharge").drop("total_amount").withColumnRenamed("total","total_amount")

// COMMAND ----------

// MAGIC %md After removing the outliers, a visualization of the dataset should show no egregious outliers. No negative value for fare amout, or in 1000s for that matter

// COMMAND ----------

//3.  Lets review status now-> after removing the outliers, a visualization of the dataset should show no egregious outliers. No negative value for fare amout, or in 1000s for that matter
cleansedDF.describe("fare_amount").show()

// COMMAND ----------

display(cleansedDF)

// COMMAND ----------

//4.  Lets persist to the curated zone
val destinationDirRootFaresCurated = "/mnt/workshop/curated/nyctaxi/model-transactions/fares/"
cleansedDF.coalesce(1).write.mode(SaveMode.Overwrite).save(destinationDirRootFaresCurated)

// COMMAND ----------

//5. Display file system to check file size - ensure you have at least 128 MB files or close
display(dbutils.fs.ls(destinationDirRootFaresCurated))

// COMMAND ----------

//6. Delete residual files from job operation (_SUCCESS, _start*, _committed*)
import com.databricks.backend.daemon.dbutils.FileInfo
dbutils.fs.ls(destinationDirRootFaresCurated + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// COMMAND ----------

// MAGIC %md
// MAGIC //7. Create hive table and compute statistics

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC USE taxi_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS model_curated_fares;
// MAGIC CREATE TABLE IF NOT EXISTS model_curated_fares
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/curated/nyctaxi/model-transactions/fares/");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC ANALYZE TABLE model_curated_fares COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %md
// MAGIC //8. Query

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_db.model_curated_fares

// COMMAND ----------

// MAGIC %md #### 2.2. Trips | Outlier detection and handling/replacing ##

// COMMAND ----------



// COMMAND ----------

display(tripsCastDF)

// COMMAND ----------

// MAGIC %md The bar chart of the trips DataFrame indicates missing points and gaps in the pickup times and passenger counts. Performing a simple filter operation to check for null and corrupt records will help verify the cause of these irregularities. 

// COMMAND ----------

//Filtering for records that have pickup and drop time as null
val nullRecordsDF=tripsCastDF.filter($"tpep_pickup_datetime".isNull or $"tpep_dropoff_datetime".isNull)
val nullRecordsCount=nullRecordsDF.count

// COMMAND ----------

// MAGIC %md The commands indicate there are a signficant number of records where the pickup/dropoff time is null. Dealing with null data can vary from case to case. In this tutorial, we will consider these null values to be corrupt. The data cleaning process will consist of removing these corrupt records.

// COMMAND ----------

//Filtering out records where pickup and dropoff date is null
val filterTripsDF=tripsCastDF.filter($"tpep_pickup_datetime".isNotNull or $"tpep_dropoff_datetime".isNotNull)

// COMMAND ----------

// MAGIC %md ##DIY: Perform cleaning on this dataset for records where passenger count is 0

// COMMAND ----------

// MAGIC %md Visualize the data again and display the passenger count.

// COMMAND ----------

display(filterTripsDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC Filter and count the number of corrupt records. Corrupt records are the ones where `passenger_count` is equal to 0

// COMMAND ----------

//Filtering for records that have passenger count as zero
val zeroPassengersDF=filterTripsDF.filter($"passenger_count"===0)
val zeroPassengersCount=zeroPassengersDF.count

// COMMAND ----------

// MAGIC %md Now, generate the DataFrame `filteredTripsDF` that does not have these corrupt records.

// COMMAND ----------

val filteredTripsDF=filterTripsDF.filter($"passenger_count"=!=0)

// COMMAND ----------

// MAGIC %md Validate your process by counting the number of clean and dirty records. The total records count should match the initial DataFrame count

// COMMAND ----------

val cleanRecordsCount=filteredTripsDF.count
val totalRecordsCount=cleanRecordsCount+zeroPassengersCount+nullRecordsCount

// COMMAND ----------

// MAGIC %md Run the following cell to test your solution

// COMMAND ----------

 if (totalRecordsCount == blobCount) "Validated!" else "No"

// COMMAND ----------

// MAGIC %md If you are not able to get the correct result you can go to `Cmd 84` and `85` at the end of the notebook to view the answers

// COMMAND ----------

//RUN THIS CELL TO LOAD THE VALIDATION RESULTS
val resultFromBlob= spark.read.option("header","true").csv(s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/nycTaxiResult.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Performing Operations on the Data &mdash; Formatting

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Once a dataset has been loaded, it's common to perform some preprocessing before proceeding to analyze it. 
// MAGIC 
// MAGIC Spark SQL's Dataset API comes with a standard library of functions called [standard functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) that allow for data manipulation. These, as well as  column-based operators like `select` or `withColumn` provide the tools to undertake the necessary transformations for data analysis.

// COMMAND ----------

// MAGIC %md ## Use Case: Categorize Users by *time of day* when Trip is Taken
// MAGIC Extract the Hour of Day from the timestamp and categorize them into 
// MAGIC - Trips taken between 0000 hours to 1200 hours.
// MAGIC - Trips taken between 1200 hours to 2400 hours.
// MAGIC We will use in-built Spark functions to filter the Months and Hours and then perform a groupBy operation to get a count of users across 4 quarters of the day.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Built-In Functions
// MAGIC 
// MAGIC Spark provides a number of <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">built-in functions</a>, many of which can be used directly with DataFrames.  Use these functions in the `filter` expressions to filter data and in `select` expressions to create derived columns.
// MAGIC 
// MAGIC 
// MAGIC #### Splitting timestamps into separate columns with date, year and month
// MAGIC 
// MAGIC The standard functions for working with dates:
// MAGIC 
// MAGIC * `dayofmonth`
// MAGIC * `year`
// MAGIC * `weekofyear`
// MAGIC * `quarter`
// MAGIC * `month`
// MAGIC * `minute`

// COMMAND ----------

// Using Inbuilt functions to extract Hours from timestamp and categorizing them according to quarters
import org.apache.spark.sql.functions._
val timeCategoryDF=filteredTripsDF.withColumn("tripHour", hour(col("tpep_pickup_datetime")))
                                  .withColumn("quarterlyTime", when($"tripHour".geq(lit(0)) and $"tripHour".lt(lit(4)),"0to4").when($"tripHour".geq(lit(4)) and $"tripHour".lt(lit(8)),"4to8").when($"tripHour".geq(lit(8)) and $"tripHour".lt(lit(12)),"8to12").when($"tripHour".geq(lit(12)) and $"tripHour".lt(lit(16)),"12to16").when($"tripHour".geq(lit(16)) and $"tripHour".lt(lit(20)),"16to20").when($"tripHour".geq(lit(20)) and $"tripHour".lt(lit(24)),"16to24"))

// COMMAND ----------

val countByTimeOfDayDF=timeCategoryDF.groupBy("quarterlyTime").count

// COMMAND ----------

// MAGIC %md To better understand the data, visualize the results. 

// COMMAND ----------

display(countByTimeOfDayDF)

// COMMAND ----------

// MAGIC %md ## DIY: Extract Date of Month from Timestamp and append to DataFrame as seperate column having name `DateOfTrip`

// COMMAND ----------

// MAGIC %md ###### Fill the commented //TO-FILL with your query and run the cell and then validate the result running by next cell 
// MAGIC for example : val tripsWithDF= //TO-FILL  to  val tripsWithDF= trips.select("tripID")

// COMMAND ----------

// MAGIC %md Use the Spark `withColumn` function to add a new column to our Trips DataFrame. The values for the new column are to be extracted from the `tpep_pickup_datetime` column. Use the inbuilt Spark functions to extract the date of month from the timestamp value.

// COMMAND ----------

// MAGIC %md
// MAGIC ![wc](https://i.ibb.co/wzZ7sx4/dfwithcolumn.png)

// COMMAND ----------

// MAGIC %md HINT: We can use in-built Spark functions to extract day of month from a column of type Timestamp. 

// COMMAND ----------

def extractDayDF(): org.apache.spark.sql.DataFrame ={  
   
val tripsWithDayOfMonthDF= filteredTripsDF.withColumn("DateOfTrip",dayofmonth($"tpep_pickup_datetime"))
    
   return(tripsWithDayOfMonthDF);
}

// COMMAND ----------

// MAGIC %md Run the following to validate your solution