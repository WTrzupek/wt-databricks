# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using SAS token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv

# COMMAND ----------

formula1dl_sas_token = dbutils.secrets.get('formula1-scope', 'formula1dlwt-sas-token')
dbutils.secrets.list('formula1-scope')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlwt.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlwt.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlwt.dfs.core.windows.net", formula1dl_sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlwt.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlwt.dfs.core.windows.net"))