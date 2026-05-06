# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Trading Volume Analytics
# MAGIC Aggregated trading volume metrics by symbol, sector, exchange, and time period.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
SILVER = "silver"
GOLD = "gold"

# COMMAND ----------

df_trades = spark.table(f"{CATALOG}.{SILVER}.trades").filter(F.col("status").isin("EXECUTED", "SETTLED"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume by Symbol & Date

# COMMAND ----------

df_volume_by_symbol = (
    df_trades
    .groupBy("trade_date", "symbol", "asset_class", "sector", "exchange")
    .agg(
        F.count("*").alias("num_trades"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("notional_value"), 2).alias("total_notional"),
        F.round(F.avg("price"), 4).alias("avg_price"),
        F.round(F.min("price"), 4).alias("min_price"),
        F.round(F.max("price"), 4).alias("max_price"),
        F.round(F.sum("commission"), 2).alias("total_commission"),
        F.countDistinct("trader_id").alias("unique_traders"),
        F.countDistinct("portfolio_id").alias("unique_portfolios"),
        F.countDistinct("execution_venue").alias("venues_used"),
    )
    .withColumn("price_range", F.round(F.col("max_price") - F.col("min_price"), 4))
    .withColumn(
        "price_range_pct",
        F.when(F.col("min_price") > 0, F.round(F.col("price_range") / F.col("min_price") * 100, 2))
        .otherwise(F.lit(0)),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume by Execution Venue

# COMMAND ----------

df_venue_stats = (
    df_trades
    .groupBy("trade_date", "execution_venue")
    .agg(
        F.count("*").alias("num_trades"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("notional_value"), 2).alias("total_notional"),
        F.round(F.avg("commission"), 4).alias("avg_commission"),
        F.countDistinct("symbol").alias("unique_symbols"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intraday Volume Profile

# COMMAND ----------

df_intraday_volume = (
    df_trades
    .groupBy("trade_date", "session", "execution_hour")
    .agg(
        F.count("*").alias("num_trades"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("notional_value"), 2).alias("total_notional"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

(
    df_volume_by_symbol.write
    .format("delta").mode("overwrite").partitionBy("trade_date")
    .saveAsTable(f"{CATALOG}.{GOLD}.trading_volume_by_symbol")
)

(
    df_venue_stats.write
    .format("delta").mode("overwrite").partitionBy("trade_date")
    .saveAsTable(f"{CATALOG}.{GOLD}.trading_volume_by_venue")
)

(
    df_intraday_volume.write
    .format("delta").mode("overwrite").partitionBy("trade_date")
    .saveAsTable(f"{CATALOG}.{GOLD}.intraday_volume_profile")
)

print("Trading volume analytics written to gold layer")

# COMMAND ----------

spark.sql(f"""
    SELECT symbol, sector,
           SUM(num_trades) as total_trades,
           SUM(total_quantity) as total_qty,
           ROUND(SUM(total_notional), 0) as total_notional,
           ROUND(AVG(price_range_pct), 2) as avg_daily_range_pct
    FROM {CATALOG}.{GOLD}.trading_volume_by_symbol
    GROUP BY symbol, sector
    ORDER BY total_notional DESC
    LIMIT 20
""").display()
