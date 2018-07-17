// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC 
// MAGIC There are multiple variables that can impact model performance - volume of data, algorithm, feature selection, training methods, hyper parameters etc.<BR>
// MAGIC In this exercise,  we will learn how hyper parameter tuning and cross validation are implemented in Spark ML.

// COMMAND ----------

import org.apache.spark.mllib.regression.LabeledPoint 
import org.apache.spark.mllib.util.MLUtils
import com.databricks.backend.daemon.dbutils.FileInfo

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

import org.apache.spark.ml.tuning.{ParamGridBuilder,CrossValidator}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0.1. Prep

// COMMAND ----------

//Source and destination
val sourceDirectory = "/mnt/data/mlw/scratchDir/model-input"
val destinationDirectory="/mnt/data/mlw/modelDir/flight-delay/randomForest/cv-tuned"

// COMMAND ----------

//Delete output from prior execution
dbutils.fs.rm(destinationDirectory,recurse=true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.0.2. Read model input in LIBSVM

// COMMAND ----------

val inputDataDF: org.apache.spark.sql.DataFrame = spark.read.format("libsvm").load(sourceDirectory).cache()
//inputDataDF: org.apache.spark.sql.DataFrame = [label: double, features: vector]

// COMMAND ----------

//Materialzie
inputDataDF.count

// COMMAND ----------

//Recap what the format looks like
inputDataDF.show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.0. Model training and tuning

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.1. Split model input into training and test datasets

// COMMAND ----------

// Split the data into training and test sets (30% held out for testing).
val Array(trainingDataset, testDataset) = inputDataDF.randomSplit(Array(0.75, 0.25), seed =1234)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.2. Instantiate RandomForestClassifer

// COMMAND ----------

// Instantiate a Random Forest classifer
val randForest = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")


// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.3. Define hyper-parameter range and number of folds

// COMMAND ----------

// Define parameter grid
val paramGrid = new ParamGridBuilder().addGrid(randForest.numTrees, Array(10, 25, 50, 100, 120)).addGrid(randForest.maxDepth, Array(3, 5, 7, 10, 15)).build()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.4. Define number of folds

// COMMAND ----------

// Specify number of folds
val numFolds = 3

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.5. Instantiate cross validator

// COMMAND ----------

// Define crossvalidator
val crossValidator = new CrossValidator().setEstimator(randForest).setEvaluator(new BinaryClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(numFolds)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.6. Train model with cross validator

// COMMAND ----------

// Run cross validation
val model = crossValidator.fit(trainingDataset)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.7. Test the model against the test data split

// COMMAND ----------

// Make predictions on test data. model is the model with combination of parameters that performed best.
val predictionsTestDF = model.transform(testDataset).select("prediction", "label", "probability" ,"features")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.8. Evaluate

// COMMAND ----------

// Compute accuracy after hyper parameter tuning and cross validation
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictionsTestDF)
println("Test Error = " + (1.0 - accuracy))
//MANUAL - 0.794150404516767
//WITH CV & HPT - 0.8085262798057732

// COMMAND ----------

// Compute area under ROC after hyper parameter tuning and cross validation
val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("probability").setMetricName("areaUnderROC")
val ROC = evaluator.evaluate(predictionsTestDF)
println("ROC on test data = " + ROC)
//MANUAL - 0.7348870570394918
//WITH CV & HPT - 0.7681621804290567


// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2.0.7. Persist tuned model

// COMMAND ----------

//Save the best model
model.bestmodel.write.overwrite().save(destinationDirectory)
//model.write.overwrite().save(destinationDirectory)