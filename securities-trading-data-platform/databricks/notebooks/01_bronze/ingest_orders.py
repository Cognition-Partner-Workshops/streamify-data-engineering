# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ingest Orders
# MAGIC Load raw order flow data from S3 into Delta Lake using Auto Loader.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
SCHEMA = "bronze"
TABLE = "orders"
S3_PATH = "s3://securities-trading-data-platform-dev/bronze/orders/"
CHECKPOINT_PATH = "s3://securities-trading-data-platform-dev/_checkpoints/bronze_orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Ingestion

# COMMAND ----------

df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(S3_PATH)
)

df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# COMMAND ----------

(
    df_bronze
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------

count = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}").count()
print(f"Bronze orders table contains {count:,} records")

spark.sql(f"""
    SELECT status, order_type, COUNT(*) as count
    FROM {CATALOG}.{SCHEMA}.{TABLE}
    GROUP BY status, order_type
    ORDER BY count DESC
""").display()
