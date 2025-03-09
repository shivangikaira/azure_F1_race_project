# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount Azure Data Lake using Service Principal
# MAGIC - Get tenant_id, client_id, client_secret from key vault.
# MAGIC - Set spark config with App/Client ID, Directory/Tenant and Secret
# MAGIC - Call file system utility mount to mount the storage.
# MAGIC - Explore the file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-app-tenant-id")
client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-app-client-id")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@kairaudemydb.dfs.core.windows.net/",
  mount_point = "/mnt/kairaudemydb/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/kairaudemydb/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/kairaudemydb/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# dbutils.fs.unmount('/mnt/kairaudemydb/demo')

# COMMAND ----------

