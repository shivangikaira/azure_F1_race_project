# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Service Principal
# MAGIC - Register Azure AD Application/Service Principal
# MAGIC - Generate a secret/password for the Apllication
# MAGIC - Set spark config with App/Client ID, Directory/Tenant and Secret
# MAGIC - Assign role storage blob contributor to the Data Lake

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-app-tenant-id")
client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-app-client-id")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-app-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.kairaudemydb.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.kairaudemydb.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.kairaudemydb.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.kairaudemydb.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.kairaudemydb.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfs://demo@kairaudemydb.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfs://demo@kairaudemydb.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

