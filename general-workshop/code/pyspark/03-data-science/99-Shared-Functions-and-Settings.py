# Databricks notebook source
# Shared settings
user_name = None

if not user_name:
  raise AttributeError("You must enter a unique user name for this workshop. " 
                       "This will avoid issues when we're all interacting with the same cluster.")

# COMMAND ----------

# Azure Machine Learning Services Settings

# COMMAND ----------

AZURE_ML_CONF = {'subscription_id': None, # Delete 'None' and enter your subscription_id here. See animation below for details
                 'resource_group': None, # Delete 'None' and enter your resource_group name here - if you don't have an AML Workspace, you can enter the desired resource group name here.
                 'workspace_name': None} # Delete 'None' and enter your workspace_name name here - if you don't have an AML Workspace, you can enter the desired workspace name here.

# COMMAND ----------

### Finding your Azure Subscription ID

# To find your Azure Subscription ID, you can navigate to [https://portal.azure.com](https://portal.azure.com), then follow the link below to see an animation on where to find it.

# https://github.com/anagha-microsoft/databricks-workshops/raw/master/general-workshop/images/8-machine-learning/3-find-azure-subscription.gif

# COMMAND ----------

# Shared Functions

# COMMAND ----------

import matplotlib.pyplot as plt

def generate_crosstab(ct, 
                     title="Location ID Populated versus Date",
                     axis_titles={'x': 'Location ID Populated', 'y': 'Date before or after July 1st, 2016'},
                     axis_labels={'x': ['No', 'Yes'], 'y': ['Before', 'After']},
                     cmap=plt.cm.Greens):
  import itertools

  fig, ax = plt.subplots()

  dt = ct.values.T[1:, :].astype('float').T

  plt.imshow(dt, interpolation='nearest', cmap=cmap)
  # plt.tight_layout()
  plt.title(title)


  plt.yticks(range(len(axis_labels['y'])), axis_labels['y'])
  plt.ylabel(axis_titles['y'])

  plt.xticks(range(len(axis_labels['x'])), axis_labels['x'])
  plt.xlabel(axis_titles['x'])
  
  thresh = dt.max() / 2.
  for i, j in itertools.product(range(dt.shape[0]), range(dt.shape[1])):
      plt.text(j, i, "{:,}".format(int(dt[i, j])),
               horizontalalignment="center",
               color="white" if dt[i, j] > thresh else "black")
             
  return fig

# COMMAND ----------

def plot_residuals(scored_data, target="duration_minutes", prediction='prediction', sample_percent=0.5):
  """
  CAUTION: This will collect the data back to the driver - 
  therefore, it's recommended to sample from the dataframe before doing that - that's what the sample percent argument is for.
  
  Plot the residual values and return a matplotlib chart.
  """
  from pyspark.sql.functions import col
  import matplotlib.pyplot as plt
  
  df = scored_data.select([col(target), col(prediction)]).withColumn("error", col(prediction) - col(target)).sample(fraction=sample_percent, withReplacement=False).toPandas()
  
  fig, ax = plt.subplots()
  
  plt.scatter(df.duration_minutes, df.error, alpha=0.5)
  plt.title("Residual Plot")
  plt.xlabel('Actual Values')
  plt.ylabel("Residual")
  
  return fig