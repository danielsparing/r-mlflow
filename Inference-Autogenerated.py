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

model_name = "[[model_name]]"
model_version = "[[model_version]]"

from mlflow import MlflowClient

client = MlflowClient()
model_uri = client.get_model_version_download_uri(model_name, model_version)
model_uri

# COMMAND ----------

# MAGIC %r
# MAGIC model_uri <- "[[model_uri]]"  # COPY FROM ABOVE
# MAGIC 
# MAGIC install.packages('mlflow')
# MAGIC library(mlflow)
# MAGIC library(sparklyr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Recreation
# MAGIC To best re-create the training environment, use the same cluster runtime and version from training. [...]

# COMMAND ----------

# MAGIC %r
# MAGIC library(randomForest)  # TODO: this library is a model dependency, should be dynamic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define input and output
# MAGIC The table path assigned to`input_table_name` will be used for batch inference and the predictions will be saved to `output_table_path`. After the table has been loaded, you can perform additional data processing, such as renaming or removing columns, to ensure the model and table schema matches.

# COMMAND ----------

# MAGIC %r
# MAGIC input_table_name = "default.[[input_table_name]]"

# COMMAND ----------

# MAGIC %r
# MAGIC # load table
# MAGIC sc <- spark_connect(method = "databricks")
# MAGIC df <- sparklyr::spark_read_table(sc = sc, name = input_table_name)

# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------

# MAGIC %r
# MAGIC mlflow_model <- mlflow_load_model(model_uri = model_uri) 

# COMMAND ----------

# MAGIC %r
# MAGIC output_df <- data.frame(mlflow_predict(mlflow_model, data = df))

# COMMAND ----------

.%md ## Save predictions
**The default output path on DBFS is accessible to everyone in this Workspace. If you want to limit access to the output you must change the path to a protected location.**
The cell below will save the output table to the specified FileStore path. `datetime.now()` is appended to the path to prevent overwriting the table in the event that this notebook is run in a batch inference job. To overwrite existing tables at the path, replace the cell below with:
```python
output_df.write.mode("overwrite").save(output_table_path)
```

### (Optional) Write predictions to Unity Catalog
If you have access to any UC catalogs, you can also save predictions to UC by specifying a table in the format `<catalog>.<database>.<table>`.
```python
output_table = "" # Example: "ml.batch-inference.[[model_name]]"
output_df.write.saveAsTable(output_table)
```

# COMMAND ----------

# MAGIC %python
# MAGIC """
# MAGIC from datetime import datetime
# MAGIC 
# MAGIC # To write to a unity catalog table, see instructions above
# MAGIC output_df.write.save(f"{output_table_path}_{datetime.now().isoformat()}".replace(":", "."))
# MAGIC """

# COMMAND ----------

# MAGIC %r
# MAGIC output_df
