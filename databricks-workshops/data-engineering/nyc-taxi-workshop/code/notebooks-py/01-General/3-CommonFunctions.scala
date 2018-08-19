// Databricks notebook source
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration

// COMMAND ----------

val prqShrinkageFactor = 0.19 //We found a saving in space of 81% with Parquet

// COMMAND ----------

def analyzeTables(databaseAndTable: String)
{
  println("Table: " + databaseAndTable)
  println("....refresh table")
  sql("REFRESH TABLE " + databaseAndTable)
  println("....analyze table")
  sql("ANALYZE TABLE " + databaseAndTable + " COMPUTE STATISTICS")
  println("....done")
}

// COMMAND ----------

def calcOutputFileCountTxtToPrq(srcDataFile: String, targetedFileSizeMB: Int): Int = {
  val fs = FileSystem.get(new Configuration())
  val estFileCount: Int = Math.floor((fs.getContentSummary(new Path(srcDataFile)).getLength * prqShrinkageFactor) / (targetedFileSizeMB * 1024 * 1024)).toInt
  if(estFileCount == 0) 1 else estFileCount
}

// COMMAND ----------

// MAGIC 
// MAGIC %python
// MAGIC # from hdfs import FileSystem, Path
// MAGIC # #from pyspark.hadoop.conf import Configuration
// MAGIC # from pyspark.sql.functions import *
// MAGIC # def calcOutputFileCountTxtToPrq(srcDataFile, targetedFileSizeMB):
// MAGIC #   FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
// MAGIC #   Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
// MAGIC #   fs = FileSystem.get(Configuration())
// MAGIC #   estFileCount = floor((fs.getContentSummary(srcDataFile).getLength * prqShrinkageFactor) / (targetedFileSizeMB * 1024 * 1024)).toInt
// MAGIC #   1 if estFileCount == 0 else estFileCount

// COMMAND ----------

// Get recursive file collection you can iterate on
def getRecursiveFileCollection(directoryPath: String): Seq[String] =
  dbutils.fs.ls(directoryPath).map(directoryItem => {
    // Work around double encoding bug
    val directoryItemPath = directoryItem.path.replace("%25", "%").replace("%25", "%")
    if (directoryItem.isDir) 
      getRecursiveFileCollection(directoryItemPath)
    else 
      Seq[String](directoryItemPath)
  }).reduce(_ ++ _)

// COMMAND ----------

// MAGIC %python
// MAGIC # Get recursive file collection you can iterate on
// MAGIC # def getRecursiveFileCollection(directoryPath):
// MAGIC #   dbutils.fs.ls(directoryPath).map(directoryItem => {
// MAGIC #     # Work around double encoding bug
// MAGIC #     directoryItemPath = directoryItem.path.replace("%25", "%").replace("%25", "%")
// MAGIC #     if (directoryItem.isDir) 
// MAGIC #       getRecursiveFileCollection(directoryItemPath)
// MAGIC #     else 
// MAGIC #       Seq[String](directoryItemPath)

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
def recursivelyDeleteSparkJobFlagFiles(directoryPath: String)
{
  
  getRecursiveFileCollection(directoryPath).foreach(directoryItemPath => {
  if (directoryItemPath.indexOf("parquet") == -1)
  {
      println("Deleting...." +  directoryItemPath)
      dbutils.fs.rm(directoryItemPath)
  }})
}


// COMMAND ----------

// MAGIC %python
// MAGIC # def recursivelyDeleteSparkJobFlagFiles(directoryPath):
// MAGIC #   for directoryItemPath in getRecursiveFileCollection(directoryPath)
// MAGIC #   getRecursiveFileCollection(directoryPath).foreach(directoryItemPath => {
// MAGIC #   if (directoryItemPath.indexOf("parquet") == -1):
// MAGIC #       print "Deleting...." +  directoryItemPath
// MAGIC #       dbutils.fs.rm(directoryItemPath)

// COMMAND ----------

dbutils.notebook.exit("Pass")

// COMMAND ----------

