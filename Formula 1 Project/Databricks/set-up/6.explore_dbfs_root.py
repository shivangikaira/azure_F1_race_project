# Databricks notebook source
# MAGIC %md
# MAGIC ###Explore DBFS Root
# MAGIC -List all the folders in DBS.
# MAGIC -Interact with DBFS File Browser
# MAGIC -Upload file to DBFS root

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/"))

# COMMAND ----------

display(spark.read.csv("dbfs:/FileStore/circuits.csv"))

# COMMAND ----------

