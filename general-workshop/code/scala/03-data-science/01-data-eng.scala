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

// MAGIC %md #### 2.2. Trips | Cleansing, outlier detection and handling/replacing ##

// COMMAND ----------

// MAGIC %md
// MAGIC 1.  Review

// COMMAND ----------

tripsCastedRenamedTripsDF.printSchema

// COMMAND ----------

tripsCastedRenamedTripsDF.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.2.1. Profile and correct timestamp fields

// COMMAND ----------

//Out of 9710124, 9650 have one or both null
tripsCastedRenamedTripsDF.filter($"pickup_timestamp".isNull or $"dropoff_timestamp".isNull).count

// COMMAND ----------

//Lets drop records without time
val filteredTripsDF = tripsCastedRenamedTripsDF.filter($"pickup_timestamp".isNotNull or $"dropoff_timestamp".isNotNull)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.2.2. Profile and correct passenger count

// COMMAND ----------

display(filteredTripsDF)

// COMMAND ----------

// MAGIC %md Visualize the data again and display the passenger count.

// COMMAND ----------

display(filteredTripsDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC Filter and count the number of corrupt records. Corrupt records are the ones where `passenger_count` is equal to 0

// COMMAND ----------

//Filtering for records that have passenger count as zero
val beforePassengerCountFix = filteredTripsDF.count
filteredTripsDF.filter($"passenger_count"===0).count

// COMMAND ----------

// MAGIC %md Now, generate the DataFrame `filteredTripsDF` that does not have these corrupt records.

// COMMAND ----------

val filteredFurther1TripsDF=filteredTripsDF.filter($"passenger_count"=!=0)
filteredFurther1TripsDF.count

// COMMAND ----------

display(filteredFurther1TripsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.2.3. Augment with temporal attributes based off of the pick up time

// COMMAND ----------

filteredFurther1TripsDF.printSchema

// COMMAND ----------

val augmentedTripDF = filteredFurther1TripsDF
.withColumn("duration",unix_timestamp($"dropoff_timestamp")-unix_timestamp($"pickup_timestamp"))
.withColumn("pickup_hour",hour($"pickup_timestamp"))
.withColumn("pickup_minute",minute($"pickup_timestamp"))
.withColumn("pickup_month",month($"pickup_timestamp"))
.withColumn("pickup_day_of_month",dayofmonth($"pickup_timestamp"))
.withColumn("pickup_day_of_week",dayofweek($"pickup_timestamp"))
.withColumn("pickup_week_of_year",weekofyear($"pickup_timestamp"))


// COMMAND ----------

display(augmentedTripDF)

// COMMAND ----------

//Lets persist to the curated zone
val destinationDirRootTripsCurated = "/mnt/workshop/curated/nyctaxi/model-transactions/trips/"
augmentedTripDF.coalesce(2).write.mode(SaveMode.Overwrite).save(destinationDirRootTripsCurated)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC USE taxi_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS model_curated_trips;
// MAGIC CREATE TABLE IF NOT EXISTS model_curated_trips
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/curated/nyctaxi/model-transactions/trips/");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC ANALYZE TABLE model_curated_trips COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_db.model_curated_trips

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Join trips and fares datasets
// MAGIC Correlate the data in two data sets using a SQL join.
// MAGIC 
// MAGIC 
// MAGIC ![SQL JOINS](http://bailiwick.io/content/images/2015/07/SQL_Join_Types-1.png)

// COMMAND ----------

// MAGIC %sql
// MAGIC describe taxi_db.model_curated_fares

// COMMAND ----------

// MAGIC %sql
// MAGIC describe taxi_db.model_curated_trips

// COMMAND ----------

// MAGIC %sql select count(*) from taxi_db.model_curated_trips

// COMMAND ----------

 spark.sql("select * from taxi_db.model_curated_fares").printSchema

// COMMAND ----------

//Execute join
val mergedTripsAndFaresDF = spark.sql("select t.trip_id,t.vendor_id,t.pickup_timestamp,t.dropoff_timestamp,t.passenger_count, t.trip_distance, t.rate_code_id, t.store_and_fwd_flag, t.pickup_locn_id, t.dropoff_locn_id, t.payment_type, t.duration, t.pickup_hour, t.pickup_minute, t.pickup_month, t.pickup_day_of_month, t.pickup_day_of_week, t.pickup_week_of_year, f.extra, f.mta_tax, f.tip_amount, f.tolls_amount, f.improvement_surcharge, f.fare_amount, f.total_amount from taxi_db.model_curated_trips t inner join taxi_db.model_curated_fares f on t.trip_id = f.trip_id")

// COMMAND ----------

//Persist to consumption zone
val destinationDirRootTripsConsumption = "/mnt/workshop/consumption/nyctaxi/model-transactions/trips/"
mergedTripsAndFaresDF.coalesce(2).write.mode(SaveMode.Overwrite).save(destinationDirRootTripsConsumption)

// COMMAND ----------

//Create external table

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;
// MAGIC USE taxi_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS model_consumption_trips;
// MAGIC CREATE TABLE IF NOT EXISTS model_consumption_trips
// MAGIC USING parquet
// MAGIC OPTIONS (path "/mnt/workshop/consumption/nyctaxi/model-transactions/trips/");
// MAGIC --USING org.apache.spark.sql.parquet
// MAGIC 
// MAGIC ANALYZE TABLE model_consumption_trips COMPUTE STATISTICS;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_db.model_consumption_trips;