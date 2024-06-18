# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join datasets to get presentation results

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_renamed = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name","race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

circuits_renamed = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location","circuit_location")

drivers_renamed = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name","driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

constructors_renamed = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name","team") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

results_renamed = spark.read.parquet(f"{processed_folder_path}/results") \
  .filter(f"file_date = '{v_file_date}'") \
  .withColumnRenamed("time", "race_time") \
  .withColumnRenamed("race_id", "result_race_id") \
  .withColumnRenamed("file_date", "result_file_date")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

race_circuits_df = races_renamed.join(circuits_renamed, races_renamed.circuit_id == circuits_renamed.circuit_id, "inner") \
    .select(races_renamed.race_id, races_renamed.race_year, races_renamed.race_name, races_renamed.race_date, circuits_renamed.circuit_location)

final_df = results_renamed.join(race_circuits_df, race_circuits_df.race_id == results_renamed.result_race_id) \
    .join(drivers_renamed, drivers_renamed.driver_id == results_renamed.driver_id) \
    .join(constructors_renamed, constructors_renamed.constructor_id == results_renamed.constructor_id ) \
    .select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", drivers_renamed["driver_nationality"], "team", "grid", "fastest_lap","race_time", "points", "position", "result_file_date") \
    .withColumn("created_date", current_timestamp()) \
    .withColumnRenamed("result_file_date", "file_date") \
    .orderBy("race_year", ascending=False)


# COMMAND ----------

write_data(final_df, "f1_presentation.race_results", "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_presentation.race_results
# MAGIC group by race_id
# MAGIC order by race_id desc