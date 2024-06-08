# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get('formula1-scope', 'formula1dlwt-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlwt.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlwt.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlwt.dfs.core.windows.net"))