# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using cluster scoped credentials
# MAGIC - Set the spark config fs.azure.account.key in the cluster
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfs://demo@kairaudemydb.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfs://demo@kairaudemydb.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

