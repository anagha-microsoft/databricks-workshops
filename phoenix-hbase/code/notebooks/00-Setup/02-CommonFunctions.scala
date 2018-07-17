// Databricks notebook source
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration

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
  val estFileCount: Int = Math.floor((fs.getContentSummary(new Path(srcDataFile)).getLength) / (targetedFileSizeMB * 1024 * 1024)).toInt
  if(estFileCount == 0) 1 else estFileCount
}

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

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
def recursivelyDeleteSparkJobFlagFiles(directoryPath: String)
{
  
  getRecursiveFileCollection(directoryPath).foreach(directoryItemPath => {
    
    if ((directoryItemPath.indexOf("parquet").toInt < 0) && 
        (directoryItemPath.indexOf("csv").toInt < 0) && 
        (directoryItemPath.indexOf("tsv").toInt < 0) && 
        (directoryItemPath.indexOf("avsc").toInt < 0) &&
        (directoryItemPath.indexOf("json").toInt < 0))
    {
        println("Deleting...." +  directoryItemPath)
        dbutils.fs.rm(directoryItemPath)
    }
  })
}


// COMMAND ----------

dbutils.notebook.exit("Pass")