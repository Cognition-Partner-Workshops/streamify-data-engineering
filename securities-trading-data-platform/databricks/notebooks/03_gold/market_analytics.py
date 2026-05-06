# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Market Analytics
# MAGIC Cross-instrument market analytics including daily summaries,
# MAGIC sector performance, and liquidity analysis.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
SILVER = "silver"
GOLD = "gold"

# COMMAND ----------

df_market = spark.table(f"{CATALOG}.{SILVER}.market_data")
df_instruments = spark.table(f"{CATALOG}.{SILVER}.instruments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Market Summary (per symbol)
# MAGIC Aggregates intraday bars into daily OHLCV with additional analytics.

# COMMAND ----------

df_daily_summary = (
    df_market
    .groupBy("symbol", "trade_date")
    .agg(
        F.first("open").alias("daily_open"),
        F.round(F.max("high"), 4).alias("daily_high"),
        F.round(F.min("low"), 4).alias("daily_low"),
        F.last("close").alias("daily_close"),
        F.sum("volume").alias("daily_volume"),
        F.round(F.avg("vwap"), 4).alias("daily_vwap"),
        F.sum("num_trades").alias("daily_num_trades"),
        F.round(F.avg("spread_bps"), 2).alias("avg_spread_bps"),
        F.round(F.avg("relative_volume"), 2).alias("avg_relative_volume"),
        F.round(F.avg("volatility_20"), 4).alias("avg_volatility"),
        F.last("sma_5").alias("sma_5"),
        F.last("sma_20").alias("sma_20"),
    )
)

# Add daily return and range
w_prev = Window.partitionBy("symbol").orderBy("trade_date")
df_daily_summary = (
    df_daily_summary
    .withColumn("prev_close", F.lag("daily_close").over(w_prev))
    .withColumn(
        "daily_return_pct",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
            F.round((F.col("daily_close") - F.col("prev_close")) / F.col("prev_close") * 100, 4),
        ).otherwise(F.lit(None)),
    )
    .withColumn("daily_range_pct", F.round((F.col("daily_high") - F.col("daily_low")) / F.col("daily_low") * 100, 4))
    # Trend signal
    .withColumn(
        "trend_signal",
        F.when(
            (F.col("sma_5").isNotNull()) & (F.col("sma_20").isNotNull()),
            F.when(F.col("sma_5") > F.col("sma_20"), "BULLISH")
            .when(F.col("sma_5") < F.col("sma_20"), "BEARISH")
            .otherwise("NEUTRAL"),
        ).otherwise("INSUFFICIENT_DATA"),
    )
    .drop("prev_close")
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sector Performance

# COMMAND ----------

# Join with instruments to get sector
df_with_sector = df_daily_summary.join(
    df_instruments.select("symbol", "sector", "asset_class"),
    on="symbol",
    how="inner",
)

df_sector_perf = (
    df_with_sector
    .filter(F.col("sector").isNotNull())
    .groupBy("trade_date", "sector")
    .agg(
        F.count("*").alias("num_instruments"),
        F.round(F.avg("daily_return_pct"), 4).alias("avg_return_pct"),
        F.round(F.sum("daily_volume"), 0).alias("total_volume"),
        F.round(F.avg("avg_spread_bps"), 2).alias("avg_spread_bps"),
        F.round(F.avg("avg_volatility"), 4).alias("avg_volatility"),
        F.sum(F.when(F.col("daily_return_pct") > 0, 1).otherwise(0)).alias("advancers"),
        F.sum(F.when(F.col("daily_return_pct") < 0, 1).otherwise(0)).alias("decliners"),
        F.sum(F.when(F.col("daily_return_pct") == 0, 1).otherwise(0)).alias("unchanged"),
    )
    .withColumn(
        "breadth_ratio",
        F.when(
            F.col("decliners") > 0,
            F.round(F.col("advancers") / F.col("decliners"), 2),
        ).otherwise(F.lit(None)),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Liquidity Analysis

# COMMAND ----------

df_liquidity = (
    df_daily_summary
    .groupBy("symbol")
    .agg(
        F.round(F.avg("daily_volume"), 0).alias("avg_daily_volume"),
        F.round(F.avg("avg_spread_bps"), 2).alias("avg_spread_bps"),
        F.round(F.avg("daily_num_trades"), 0).alias("avg_daily_trades"),
        F.round(F.avg("avg_relative_volume"), 2).alias("avg_relative_volume"),
        F.count("*").alias("trading_days"),
    )
    .withColumn(
        "liquidity_tier",
        F.when(F.col("avg_daily_volume") > 500_000, "HIGH")
        .when(F.col("avg_daily_volume") > 100_000, "MEDIUM")
        .otherwise("LOW"),
    )
    .withColumn(
        "liquidity_score",
        F.round(
            F.log10(F.col("avg_daily_volume") + 1) / F.greatest(F.col("avg_spread_bps"), F.lit(0.1)),
            2,
        ),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

(
    df_daily_summary.write
    .format("delta").mode("overwrite").partitionBy("trade_date")
    .saveAsTable(f"{CATALOG}.{GOLD}.daily_market_summary")
)

(
    df_sector_perf.write
    .format("delta").mode("overwrite").partitionBy("trade_date")
    .saveAsTable(f"{CATALOG}.{GOLD}.sector_performance")
)

(
    df_liquidity.write
    .format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.liquidity_analysis")
)

print("Market analytics written to gold layer")

# COMMAND ----------

spark.sql(f"""
    SELECT symbol, liquidity_tier,
           avg_daily_volume, avg_spread_bps,
           avg_daily_trades, liquidity_score
    FROM {CATALOG}.{GOLD}.liquidity_analysis
    ORDER BY liquidity_score DESC
    LIMIT 20
""").display()
