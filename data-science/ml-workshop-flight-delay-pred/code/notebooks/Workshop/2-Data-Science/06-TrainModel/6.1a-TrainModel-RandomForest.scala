// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC In this exercise, we will create a machine learning model in Spark MLLib, ML API, and persist the model for later use

// COMMAND ----------

import org.apache.spark.mllib.regression.LabeledPoint 
import org.apache.spark.mllib.util.MLUtils
import com.databricks.backend.daemon.dbutils.FileInfo

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

import org.apache.spark.ml.tuning.{ParamGridBuilder,CrossValidator}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1. Create dataframe of model input

// COMMAND ----------

//Source and destination
val sourceDirectory = "/mnt/data/mlw/scratchDir/model-input"
val destinationDirectory="/mnt/data/mlw/modelDir/flight-delay/randomForest/manually-tuned"

// COMMAND ----------

val inputDataDF: org.apache.spark.sql.DataFrame = spark.read.format("libsvm").load(sourceDirectory).cache
//inputDataDF: org.apache.spark.sql.DataFrame = [label: double, features: vector]

// COMMAND ----------

//Materialize
inputDataDF.count

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 2. Split into training and test data

// COMMAND ----------

// Split the data into training and test sets (30% held out for testing).
val Array(trainingDataset, testDataset) = inputDataDF.randomSplit(Array(0.75, 0.25), seed =1234)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 3. Instantiate classifier

// COMMAND ----------

val randForest = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setNumTrees(100)
  .setMaxDepth(10)
  .setMaxBins(160)


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 4. Train the model with the training data split

// COMMAND ----------

// Train the model
val model = randForest.fit(trainingDataset)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 5. Test the model against the test data split

// COMMAND ----------

// Make predictions on the test dataset
val predictions = model.transform(testDataset)

// Select example rows to display.
predictions.select("prediction", "label", "probability" ,"features").show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 6. Review metrics

// COMMAND ----------

// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))
//0.794150404516767

// COMMAND ----------

//* Binary classification evaluation metrics*//
val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("probability").setMetricName("areaUnderROC")
val ROC = evaluator.evaluate(predictions)
println("ROC on test data = " + ROC)
//0.7348870570394918

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 7. Print model

// COMMAND ----------

//Print model 
println("Learned classification forest model:\n" + model.toDebugString)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 8. Persist model

// COMMAND ----------

model.write.overwrite.save(destinationDirectory)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 9. Check feature importance

// COMMAND ----------

//FEATURE IMPORTANCE
model.featureImportances

//0 - origin_airport_id - 0.061617093986384194
//1 - flight_month - 0.04194211627845236
//2 - flight_day_of_month - 0.11509911942769702
//3 - flight_day_of_week - 0.061531660019990865
//4 - flight_dep_hour - 0.4371281475075368
//5 - carrier_indx - 0.05770819357125712
//6 - dest_airport_id - 0.01769814715174663
//7 - wind_speed - 0.02170460496570623
//8 - sea_level_pressure - 0.12510959002658545
//9 - hourly_precip - 0.060461327064643425

// COMMAND ----------

