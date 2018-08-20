// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Download NYC taxi public dataset

// COMMAND ----------

// MAGIC %md
// MAGIC # DO NOT RUN THIS AT THE WORKSHOP

// COMMAND ----------

//imports
import sys.process._
import scala.sys.process.ProcessLogger

// COMMAND ----------

//================================================================================
// 2. DOWNLOAD LOOKUP DATA
//================================================================================
dbutils.fs.rm("file:/tmp/taxi+_zone_lookup.csv")
"wget -P /tmp https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv" !!
val localPath="file:/tmp/taxi+_zone_lookup.csv"
val wasbPath="/mnt/data/nyctaxi/stagingDir/reference-data/taxi_zone_lookup.csv"

display(dbutils.fs.ls(localPath))
dbutils.fs.cp(localPath, wasbPath)
dbutils.fs.rm(localPath)
display(dbutils.fs.ls(wasbPath))

// COMMAND ----------

//================================================================================
// 2. DOWNLOAD 2017 TRANSACTIONAL DATA
//================================================================================
val cabTypes = Seq("yellow", "green", "fhv")
for (cabType <- cabTypes) {
  for (i <- 1 to 6) 
  {
    val fileName = cabType + "_tripdata_2017-0" + i + ".csv"
    val wasbPath="/mnt/data/nyctaxi/stagingDir/transactional-data/year=2017/month=0" + i + "/type=" + cabType + "/"
    val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
    println(wgetToExec)
  
    wgetToExec !!
  
    val localPath="file:/tmp/" + fileName
    dbutils.fs.mkdirs(wasbPath)
    dbutils.fs.cp(localPath, wasbPath)
    dbutils.fs.rm(localPath)
    display(dbutils.fs.ls(wasbPath))
  }
}

// COMMAND ----------

//================================================================================
// 3. DOWNLOAD 2014-16 TRANSACTIONAL DATA
//================================================================================
val cabTypes = Seq("yellow", "green", "fhv")

for (cabType <- cabTypes) {
  for (j <- 2014 to 2016)
  {
    for (i <- 1 to 12) 
    {
      val fileName = cabType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
      println(fileName)
      val wasbPath="/mnt/data/nyctaxi/stagingDir/transactional-data/year=" + j + "/month=" +  "%02d".format(i) + "/type=" + cabType + "/"
      val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
      println(wgetToExec)

      wgetToExec !!

      val localPath="file:/tmp/" + fileName
      dbutils.fs.mkdirs(wasbPath)
      dbutils.fs.cp(localPath, wasbPath)
      dbutils.fs.rm(localPath)
      display(dbutils.fs.ls(wasbPath))
    } 
  }
}

// COMMAND ----------

//================================================================================
// 4. DOWNLOAD 2013 TRANSACTIONAL DATA - JAN to JULY
//================================================================================

val j = "2013"
val cabType = "yellow"
for (i <- 1 to 7) 
{
  val fileName = cabType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
  println(fileName)
  val wasbPath="/mnt/data/nyctaxi/stagingDir/transactional-data/year=" + j + "/month=" +  "%02d".format(i) + "/type=" + cabType + "/"
  val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
  println(wgetToExec)

  wgetToExec !!

  val localPath="file:/tmp/" + fileName
  dbutils.fs.mkdirs(wasbPath)
  dbutils.fs.cp(localPath, wasbPath)
  dbutils.fs.rm(localPath)
  display(dbutils.fs.ls(wasbPath))
} 


// COMMAND ----------

//================================================================================
// 4b. DOWNLOAD 2013 TRANSACTIONAL DATA - AUG - DEC
//================================================================================

val j = "2013"
val cabTypes = Seq("yellow", "green")
for (cabType <- cabTypes) {
  for (i <- 8 to 12) 
  {
    val fileName = cabType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
    println(fileName)
    val wasbPath="/mnt/data/nyctaxi/stagingDir/transactional-data/year=" + j + "/month=" +  "%02d".format(i) + "/type=" + cabType + "/"
    val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
    println(wgetToExec)

    wgetToExec !!

    val localPath="file:/tmp/" + fileName
    dbutils.fs.mkdirs(wasbPath)
    dbutils.fs.cp(localPath, wasbPath)
    dbutils.fs.rm(localPath)
    display(dbutils.fs.ls(wasbPath))
  } 
}

// COMMAND ----------

//================================================================================
// 5. DOWNLOAD 2009-12 TRANSACTIONAL DATA
//================================================================================

val cabType = "yellow"
for (j <- 2009 to 2012)
{
  for (i <- 1 to 12) 
  {
    val fileName = cabType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
    println(fileName)
    val wasbPath="/mnt/data/nyctaxi/stagingDir/transactional-data/year=" + j + "/month=" +  "%02d".format(i) + "/type=" + cabType + "/"
    val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
    println(wgetToExec)

    wgetToExec !!

    val localPath="file:/tmp/" + fileName
    dbutils.fs.mkdirs(wasbPath)
    dbutils.fs.cp(localPath, wasbPath)
    dbutils.fs.rm(localPath)
    display(dbutils.fs.ls(wasbPath))
  } 
}

// COMMAND ----------

