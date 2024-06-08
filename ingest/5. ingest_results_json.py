# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

results_schema = StructType(fields=[
  StructField("resultId", IntegerType(), False),
  StructField("raceId", IntegerType(), False),
  StructField("driverId", IntegerType(), False),
  StructField("constructorId", IntegerType(), False),
  StructField("number", IntegerType(), True),
  StructField("grid", IntegerType(), False),
  StructField("position", IntegerType(), True),
  StructField("positionText", StringType(), False),
  StructField("positionOrder", IntegerType(), False),
  StructField("points", FloatType(), False),
  StructField("laps", IntegerType(), False),
  StructField("time", StringType(), True),
  StructField("miliseconds", IntegerType(), True),
  StructField("fastestLap", IntegerType(), True),
  StructField("rank", IntegerType(), True),
  StructField("fastestLapTime", StringType(), True),
  StructField("fastestLapSpeed", StringType(), True),
  StructField("statusId", IntegerType(), False)
])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"/mnt/formula1dlwt/raw/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - drop unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

results_dropped_df = results_df.drop(col('statusId'))

# COMMAND ----------

display(results_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

results_fnal_df = results_dropped_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_fnal_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write data to datalake as parquet

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------

# for race_id_list in results_fnal_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_fnal_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

write_data(results_fnal_df, "f1_processed.results", "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc