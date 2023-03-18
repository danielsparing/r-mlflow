# Databricks notebook source
# MAGIC %md
# MAGIC This is an auto-generated notebook to perform batch inference on a Spark DataFrame using a selected model from the model registry. This feature is in preview, and we would greatly appreciate any feedback through this form: https://databricks.sjc1.qualtrics.com/jfe/form/SV_1H6Ovx38zgCKAR0.
# MAGIC 
# MAGIC ## Instructions:
# MAGIC 1. To best re-create the training environment, use the same cluster runtime and version from training.
# MAGIC 2. Add additional data processing on your loaded table to match the model schema if necessary (see the "Define input and output" section below).
# MAGIC 3. "Run All" the notebook.
# MAGIC 4. Note: If the `%pip` does not work for your model (i.e. it does not have a `requirements.txt` file logged), modify to use `%conda` if possible.

# COMMAND ----------

# MAGIC %python
# MAGIC model_name = "[[model_name]]"
# MAGIC model_version = "[[model_version]]"
# MAGIC 
# MAGIC from mlflow import MlflowClient
# MAGIC 
# MAGIC client = MlflowClient()
# MAGIC model_uri = client.get_model_version_download_uri(model_name, model_version)
# MAGIC model_uri

# COMMAND ----------

model_uri <- "[[model_uri]]"  # COPY FROM ABOVE

install.packages('mlflow')
library(mlflow)
library(sparklyr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Recreation
# MAGIC To best re-create the training environment, use the same cluster runtime and version from training. [...]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define input and output
# MAGIC The table path assigned to`input_table_name` will be used for batch inference and the predictions will be saved to `output_table_path`. After the table has been loaded, you can perform additional data processing, such as renaming or removing columns, to ensure the model and table schema matches.

# COMMAND ----------

input_table_name = "[[input_table_name]]"

# COMMAND ----------

# load table
sc <- spark_connect(method = "databricks")
df <- sparklyr::spark_read_table(sc = sc, name = input_table_name)

# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------

mlflow_model <- mlflow_load_model(model_uri = model_uri) 

# COMMAND ----------

output_df <- data.frame(mlflow_predict(mlflow_model, data = df))

# COMMAND ----------

# MAGIC %md ## Save predictions
# MAGIC **The default output path on DBFS is accessible to everyone in this Workspace. If you want to limit access to the output you must change the path to a protected location.**
# MAGIC The cell below will save the output table to the specified FileStore path. `datetime.now()` is appended to the path to prevent overwriting the table in the event that this notebook is run in a batch inference job. To overwrite existing tables at the path, replace the cell below with:
# MAGIC ```python
# MAGIC output_df.write.mode("overwrite").save(output_table_path)
# MAGIC ```
# MAGIC 
# MAGIC ### (Optional) Write predictions to Unity Catalog
# MAGIC If you have access to any UC catalogs, you can also save predictions to UC by specifying a table in the format `<catalog>.<database>.<table>`.
# MAGIC ```python
# MAGIC output_table = "" # Example: "ml.batch-inference.[[model_name]]"
# MAGIC output_df.write.saveAsTable(output_table)
# MAGIC ```

# COMMAND ----------

# MAGIC %python
# MAGIC """
# MAGIC from datetime import datetime
# MAGIC 
# MAGIC # To write to a unity catalog table, see instructions above
# MAGIC output_df.write.save(f"{output_table_path}_{datetime.now().isoformat()}".replace(":", "."))
# MAGIC """

# COMMAND ----------

output_df

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
