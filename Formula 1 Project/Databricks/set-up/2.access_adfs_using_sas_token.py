# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using SAS tokens
# MAGIC - Set the spark config for SAS Token
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_sas_token = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.kairaudemydb.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.kairaudemydb.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.kairaudemydb.dfs.core.windows.net",formula1dl_account_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfs://demo@kairaudemydb.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfs://demo@kairaudemydb.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

