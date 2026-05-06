# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ingest Market Data
# MAGIC Load raw OHLCV market data from S3 into Delta Lake.
# MAGIC This is the highest-volume dataset — partitioned by date for performance.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
SCHEMA = "bronze"
TABLE = "market_data"
S3_PATH = "s3://securities-trading-data-platform-dev/bronze/market_data/"
CHECKPOINT_PATH = "s3://securities-trading-data-platform-dev/_checkpoints/bronze_market_data"

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
    .withColumn("_trade_date", F.to_date(F.col("timestamp")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write with Date Partitioning

# COMMAND ----------

(
    df_bronze
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .partitionBy("_trade_date")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------

count = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}").count()
print(f"Bronze market_data table contains {count:,} records")

spark.sql(f"""
    SELECT _trade_date, COUNT(*) as num_bars, COUNT(DISTINCT symbol) as num_symbols
    FROM {CATALOG}.{SCHEMA}.{TABLE}
    GROUP BY _trade_date
    ORDER BY _trade_date DESC
    LIMIT 10
""").display()
