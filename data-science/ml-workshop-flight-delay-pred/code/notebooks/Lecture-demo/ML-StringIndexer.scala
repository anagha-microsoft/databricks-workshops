// Databricks notebook source
import org.apache.spark.ml.feature.StringIndexer

val df: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = sqlContext.sql("""
select 
*
from flight_db.carrier_master
""").na.drop.cache

val indexer: org.apache.spark.ml.feature.StringIndexer = new StringIndexer()
  .setInputCol("carrier_cd")
  .setOutputCol("carrier_cd_index")

val indexed: org.apache.spark.sql.DataFrame = indexer.fit(df).transform(df)
indexed.show()

// COMMAND ----------

import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

val inputColSchema = indexed.schema(indexer.getOutputCol)
println(s"StringIndexer will store labels in output column metadata: " +
    s"${Attribute.fromStructField(inputColSchema).toString}\n")

// COMMAND ----------

val converter: org.apache.spark.ml.feature.IndexToString = new IndexToString()
  .setInputCol("carrier_cd_index")
  .setOutputCol("carrier_cd_reconverted")

val converted: org.apache.spark.sql.DataFrame = converter.transform(indexed)

converted.show()

// COMMAND ----------

println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
    s"column '${converter.getOutputCol}' using labels in metadata")
converted.select("carrier_cd_index", "carrier_cd_reconverted").show()