# Databricks notebook source
# MAGIC %md # MLflow quickstart: tracking
# MAGIC 
# MAGIC This notebook creates a Random Forest model on a simple dataset and uses the MLflow Tracking API to log the model, selected model parameters and evaluation metrics, and other artifacts.
# MAGIC 
# MAGIC ## Requirements
# MAGIC * This notebook requires Databricks Runtime 6.4 or above, or Databricks Runtime 6.4 ML or above.

# COMMAND ----------

# MAGIC %md This notebook does not use distributed processing, so you can use the R `install.packages()` function to install packages on the driver node only.  
# MAGIC To take advantage of distributed processing, you must install packages on all nodes in the cluster by creating a cluster library. See "Install a library on a cluster" ([AWS](https://docs.databricks.com/libraries/cluster-libraries.html#install-a-library-on-a-cluster)|[Azure](https://docs.microsoft.com/en-us/azure/databricks/libraries/cluster-libraries#--install-a-library-on-a-cluster)|[GCP](https://docs.gcp.databricks.com/libraries/cluster-libraries.html#install-a-library-on-a-cluster)).

# COMMAND ----------

install.packages("mlflow")

# COMMAND ----------

# MAGIC %md
# MAGIC You can run this notebook as is for MLflow 2.0 workflows, but if you are using MLflow 1.x, uncomment the `install_mlflow()` command to ensure the appropriate MLflow packages are installed.

# COMMAND ----------

library(mlflow)
# install_mlflow() 

# COMMAND ----------

# MAGIC %md Import the required libraries.  
# MAGIC 
# MAGIC This notebook uses the R library `carrier` to serialize the predict method of the trained model, so that it can be loaded back into memory later. For more information, see the [`carrier` github repo](https://github.com/r-lib/carrier). 

# COMMAND ----------

install.packages("carrier")
# install.packages("e1071")
install.packages("xgboost")

library(MASS)
library(caret)
# library(e1071)
library(xgboost)
# library(SparkR)
library(carrier)
library(sparklyr)

# COMMAND ----------

# create Delta Table to be scored
sc <- sparklyr::spark_connect(method = "databricks")
iris_ref <- copy_to(sc, df = Pima.te[,1:7], temporary = TRUE, overwrite = TRUE)
sparklyr::spark_write_table(iris_ref, name = "iris_score", mode = "overwrite") 

# COMMAND ----------

with(mlflow_start_run(), {
    
  # Set the model parameters
  nrounds <- 2

  # Create and train model
  bst <- xgboost(data=as.matrix(Pima.tr[,1:7]), label=Pima.tr[,8], nrounds=2)
  
  # Use the model to make predictions on the test dataset
  pred <- predict(bst, newdata=as.matrix(Pima.te[,1:7]))
  
  # Log the model parameters used for this run
  mlflow_log_param("nrounds", nrounds)
  
  # Log the model
  # The crate() function from the R package "carrier" stores the model as a function
  # predictor <- crate(function(x) stats::predict(object = bst, newData = .x), bst = bst)
  predictor <- crate(function(x) stats::predict(object = bst, newData = .x), bst = bst)
  mlflow_log_model(predictor, "model")     
  
    
})

# COMMAND ----------

predictor(as.matrix(Pima.te[,1:7]))

# COMMAND ----------

# MAGIC %md To view the results, click **Experiment** at the upper right of this page. The Experiments sidebar appears. This sidebar displays the parameters and metrics for each run of this notebook. Click the circular arrows icon to refresh the display to include the latest runs. 
# MAGIC 
# MAGIC When you click the square icon with the arrow to the right of the date and time of the run, the Runs page opens in a new tab. This page shows all of the information that was logged from the run. Scroll down to the Artifacts section to find the logged model and plot.
# MAGIC 
# MAGIC For more information, see "View notebook experiment" ([AWS](https://docs.databricks.com/applications/mlflow/tracking.html#view-notebook-experiment)|[Azure](https://docs.microsoft.com/azure/databricks/applications/mlflow/tracking#view-notebook-experiment)|[GCP](https://docs.gcp.databricks.com/applications/mlflow/tracking.html#view-notebook-experiment)).

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
