# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ingest Trades
# MAGIC Load raw trade execution data from S3 into Delta Lake.
# MAGIC Uses Auto Loader (cloudFiles) for incremental ingestion.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "trading_platform"
SCHEMA = "bronze"
TABLE = "trades"
S3_PATH = "s3://securities-trading-data-platform-dev/bronze/trades/"
CHECKPOINT_PATH = "s3://securities-trading-data-platform-dev/_checkpoints/bronze_trades"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest with Auto Loader
# MAGIC Auto Loader (cloudFiles) automatically detects and processes new files
# MAGIC arriving in the S3 path, providing exactly-once ingestion guarantees.

# COMMAND ----------

df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(S3_PATH)
)

# Add ingestion metadata
df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta as Streaming Table

# COMMAND ----------

(
    df_bronze
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Process all available files then stop
    .toTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------

count = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}").count()
print(f"Bronze trades table contains {count:,} records")

spark.sql(f"""
    SELECT trade_date, COUNT(*) as num_trades, SUM(notional_value) as total_notional
    FROM {CATALOG}.{SCHEMA}.{TABLE}
    GROUP BY trade_date
    ORDER BY trade_date DESC
    LIMIT 10
""").display()
