# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

dbutils.secrets.list('formula1-scope')

# COMMAND ----------

def f_mount(container, storage):
    # Get secrets
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlwt-sp-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlwt-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlwt-sp-client-secret')   

    # Set configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #Mount
    if any(mount.mountPoint == f"/mnt/{storage}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage}/{container}")

    dbutils.fs.mount(
    source = f"abfss://{container}@{storage}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage}/{container}",
    extra_configs = configs)

# COMMAND ----------

f_mount("demo","formula1dlwt")
f_mount("presentation","formula1dlwt")
f_mount("processed","formula1dlwt")
f_mount("raw","formula1dlwt")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlwt/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlwt/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())