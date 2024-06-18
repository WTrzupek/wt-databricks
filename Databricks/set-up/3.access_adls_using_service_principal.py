# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/password for the App
# MAGIC 3. Set Spark config with App/Client Id, Directory/Tenant ID & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

dbutils.secrets.list('formula1-scope')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlwt.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlwt.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlwt.dfs.core.windows.net", dbutils.secrets.get('formula1-scope','formula1dlwt-sp-client-id'))
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlwt.dfs.core.windows.net", dbutils.secrets.get('formula1-scope','formula1dlwt-sp-client-secret'))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlwt.dfs.core.windows.net", f"https://login.microsoftonline.com/{dbutils.secrets.get('formula1-scope','formula1dlwt-tenant-id')}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlwt.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlwt.dfs.core.windows.net"))