# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze.bronze_sch;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver.silver_sch;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold.gold_sch;

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.energydatastore01.dfs.core.windows.net",
    "W/LaBmje7OrDjjUBg++TndrJ4dGxmvEKtwOzyIPRnr1xzxAvOVx0jEpNfckec7BLYBmOTbPIwW4i+AStC5CIaw=="
)
print("Storage account authentication configured successfully.")

# COMMAND ----------

base_transformed_path = "abfss://energycontainer01@energydatastore01.dfs.core.windows.net/Transformed/"

energy_df = (
    spark.read
    .format("parquet")
    .load(base_transformed_path + "tariff_metrics_stream_v2.parquet")
)

display(energy_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, expr

tariff_bronze = (
    energy_df
    .withColumn("bronze_ingestion_time", current_timestamp())
    .withColumn("source_file", expr("_metadata.file_path"))
    .withColumn("batch_id", expr("uuid()"))
)

display(tariff_bronze)

# COMMAND ----------

tariff_bronze = (
    tariff_bronze
    .withWatermark("reading_timestamp", "2 hours")
)
display(tariff_bronze)

# COMMAND ----------

spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)

print("Schema evolution enabled successfully.")

# COMMAND ----------

target_table = "bronze.bronze_sch.Tariff_Metrics"

try:

    (
        tariff_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Successfully loaded into {target_table}")

except Exception as e:

    print(f"Bronze Load Failed : {target_table}")
    print(e)

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {target_table}").display()

# COMMAND ----------

# Create the central log table if it doesn't exist yet
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.bronze_sch.bronze_audit_logs (
        process_name STRING,
        source_file STRING,
        target_table STRING,
        records_processed LONG,
        status STRING,
        error_message STRING,
        log_timestamp TIMESTAMP
    )
    USING delta
""")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, expr
from datetime import datetime
import sys

PROCESS_NAME = "Ingest_Energy_Usage"
FILE_NAME = "tariff_metrics_stream_v2.parquet"
TARGET_TABLE = "bronze.bronze_sch.Tariff_Metrics"

try:
    # 1. Read Data (Assuming energy_df is your spark.read dataframe)
    tariff_bronze = (
        energy_df
        .withColumn("bronze_ingestion_time", current_timestamp())
        .withColumn("source_file", expr("_metadata.file_path"))
    )
    
    # 2. Write Data to Delta
    tariff_bronze.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(TARGET_TABLE)
        
    # 3. Capture Row Count for Audit
    inserted_rows = tariff_bronze.count()
    
    # 4. Write SUCCESS Log (Using standard datetime.now() instead of the PySpark function)
    current_time = datetime.now()
    audit_df = spark.createDataFrame([(
        PROCESS_NAME, FILE_NAME, TARGET_TABLE, inserted_rows, "SUCCESS", None, current_time
    )], schema="process_name STRING, source_file STRING, target_table STRING, records_processed LONG, status STRING, error_message STRING, log_timestamp TIMESTAMP")
    
    audit_df.write.format("delta").mode("append").saveAsTable("bronze.bronze_sch.bronze_audit_logs")
    print(f"Successfully processed {inserted_rows} records.")

except Exception as e:
    # 5. Write FAILURE Log if anything breaks
    error_msg = str(e)
    current_time = datetime.now()
    
    error_df = spark.createDataFrame([(
        PROCESS_NAME, FILE_NAME, TARGET_TABLE, 0, "FAILED", error_msg, current_time
    )], schema="process_name STRING, source_file STRING, target_table STRING, records_processed LONG, status STRING, error_message STRING, log_timestamp TIMESTAMP")
    
    error_df.write.format("delta").mode("append").saveAsTable("bronze.bronze_sch.bronze_audit_logs")
    print(f"Pipeline Failed: {error_msg}")