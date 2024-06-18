# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def write_data(df, table, partiton_by):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df_schema = df.schema.names

    if partiton_by in df_schema:
        partiton_by_index = df_schema.index(partiton_by)
        df_schema.pop(partiton_by_index)

    df_schema.append(partiton_by)
    df = df.select(df_schema)

    if (spark._jsparkSession.catalog().tableExists(table)):
        df.write.mode("overwrite").insertInto(table)
    else:
        df.write.mode("overwrite").partitionBy(partiton_by).format("parquet").saveAsTable(table)