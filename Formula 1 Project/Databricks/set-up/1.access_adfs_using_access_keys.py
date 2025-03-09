# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using access keys
# MAGIC - Set the spark config fs.azure.account.key
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key=dbutils.secrets.get(scope="formula1-scope", key="formula1dl-account-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.kairaudemydb.dfs.core.windows.net", formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfs://demo@kairaudemydb.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfs://demo@kairaudemydb.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

