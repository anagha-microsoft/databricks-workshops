# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Machine Learning Services Settings

# COMMAND ----------

AZURE_ML_CONF = {'subscription_id': None, # Delete 'None' and enter your subscription_id here. See animation below for details
                 'resource_group': None, # Delete 'None' and enter your resource_group name here - if you don't have an AML Workspace, you can enter the desired resource group name here.
                 'workspace_name': None} # Delete 'None' and enter your workspace_name name here - if you don't have an AML Workspace, you can enter the desired workspace name here.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding your Azure Subscription ID
# MAGIC 
# MAGIC To find your Azure Subscription ID, you can navigate to [https://portal.azure.com](https://portal.azure.com), then follow the steps shown below.
# MAGIC ![](https://github.com/anagha-microsoft/databricks-workshops/raw/master/general-workshop/images/8-machine-learning/3-find-azure-subscription.gif)

# COMMAND ----------

# MAGIC %md
# MAGIC # Shared Functions

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