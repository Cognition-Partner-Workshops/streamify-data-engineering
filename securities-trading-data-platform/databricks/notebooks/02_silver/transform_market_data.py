# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Transform Market Data
# MAGIC Cleanse OHLCV data, validate price integrity, and compute technical indicators.
# MAGIC - Filter out bars with invalid prices (high < low, etc.)
# MAGIC - Calculate intrabar metrics (true range, typical price)
# MAGIC - Compute rolling averages and volatility

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
BRONZE = "bronze"
SILVER = "silver"
TABLE = "market_data"

# COMMAND ----------

df_bronze = spark.table(f"{CATALOG}.{BRONZE}.{TABLE}")
print(f"Bronze market_data: {df_bronze.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality & Technical Indicators

# COMMAND ----------

df_clean = (
    df_bronze
    .drop("_ingested_at", "_source_file", "_trade_date")
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("trade_date", F.to_date("timestamp"))
    # Price integrity checks
    .filter(
        (F.col("open") > 0)
        & (F.col("high") > 0)
        & (F.col("low") > 0)
        & (F.col("close") > 0)
        & (F.col("high") >= F.col("low"))
        & (F.col("high") >= F.col("open"))
        & (F.col("high") >= F.col("close"))
        & (F.col("low") <= F.col("open"))
        & (F.col("low") <= F.col("close"))
        & (F.col("volume") >= 0)
    )
)

# Window for rolling calculations (per symbol, ordered by timestamp)
w20 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-19, 0)
w5 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-4, 0)

df_silver = (
    df_clean
    # Intrabar metrics
    .withColumn("typical_price", F.round((F.col("high") + F.col("low") + F.col("close")) / 3, 4))
    .withColumn("true_range", F.col("high") - F.col("low"))
    .withColumn("bar_range_pct", F.round((F.col("high") - F.col("low")) / F.col("close") * 100, 4))
    .withColumn("return_pct", F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 4))
    # Rolling averages
    .withColumn("sma_5", F.round(F.avg("close").over(w5), 4))
    .withColumn("sma_20", F.round(F.avg("close").over(w20), 4))
    .withColumn("vol_sma_20", F.round(F.avg("volume").over(w20), 0))
    # Volatility (rolling std of returns)
    .withColumn("volatility_20", F.round(F.stddev("return_pct").over(w20), 4))
    # Relative volume
    .withColumn(
        "relative_volume",
        F.when(F.col("vol_sma_20") > 0, F.round(F.col("volume") / F.col("vol_sma_20"), 2))
        .otherwise(F.lit(None)),
    )
    # Spread metrics
    .withColumn("spread_bps", F.round(F.col("spread") / F.col("close") * 10000, 2))
    .withColumn("_processed_at", F.current_timestamp())
)

print(f"Silver market_data: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver

# COMMAND ----------

target_table = f"{CATALOG}.{SILVER}.{TABLE}"

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("trade_date")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Written to {target_table}")

# COMMAND ----------

spark.sql(f"""
    SELECT symbol, trade_date,
           ROUND(MIN(low), 2) as day_low,
           ROUND(MAX(high), 2) as day_high,
           ROUND(AVG(spread_bps), 2) as avg_spread_bps,
           SUM(volume) as total_volume
    FROM {target_table}
    WHERE trade_date = (SELECT MAX(trade_date) FROM {target_table})
    GROUP BY symbol, trade_date
    ORDER BY total_volume DESC
    LIMIT 10
""").display()
