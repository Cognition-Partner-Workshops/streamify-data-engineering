# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ingest Positions
# MAGIC Load raw end-of-day position snapshots from S3 into Delta Lake.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
SCHEMA = "bronze"
TABLE = "positions"
S3_PATH = "s3://securities-trading-data-platform-dev/bronze/positions/"
CHECKPOINT_PATH = "s3://securities-trading-data-platform-dev/_checkpoints/bronze_positions"

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

count = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}").count()
print(f"Bronze positions table contains {count:,} records")

spark.sql(f"""
    SELECT as_of_date, COUNT(DISTINCT portfolio_id) as portfolios, COUNT(*) as positions
    FROM {CATALOG}.{SCHEMA}.{TABLE}
    GROUP BY as_of_date
    ORDER BY as_of_date DESC
    LIMIT 10
""").display()
