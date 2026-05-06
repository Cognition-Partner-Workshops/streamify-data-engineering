# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Portfolio Summary
# MAGIC Executive-level portfolio analytics including AUM, diversification, and performance.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
SILVER = "silver"
GOLD = "gold"

# COMMAND ----------

df_positions = spark.table(f"{CATALOG}.{SILVER}.positions")
df_trades = spark.table(f"{CATALOG}.{SILVER}.trades").filter(F.col("status").isin("EXECUTED", "SETTLED"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Portfolio Summary (Latest Snapshot)

# COMMAND ----------

# Get the latest snapshot date
latest_date = df_positions.agg(F.max("as_of_date")).collect()[0][0]
df_latest = df_positions.filter(F.col("as_of_date") == latest_date)

df_portfolio_summary = (
    df_latest
    .groupBy("portfolio_id", "as_of_date")
    .agg(
        # AUM
        F.round(F.sum("market_value"), 2).alias("total_aum"),
        F.count("*").alias("num_positions"),
        F.countDistinct("asset_class").alias("asset_classes"),
        F.countDistinct("sector").alias("sectors"),
        # P&L
        F.round(F.sum("unrealized_pnl"), 2).alias("total_unrealized_pnl"),
        F.round(F.sum("realized_pnl"), 2).alias("total_realized_pnl"),
        F.round(F.sum("total_pnl"), 2).alias("total_pnl"),
        F.round(F.avg("unrealized_pnl_pct"), 2).alias("avg_return_pct"),
        # Concentration
        F.round(F.max("weight"), 2).alias("max_position_weight"),
        F.round(F.stddev("weight"), 2).alias("weight_std_dev"),
        # Risk
        F.sum(F.when(F.col("pnl_risk_class") == "HIGH_LOSS", 1).otherwise(0)).alias("high_loss_positions"),
        F.sum(F.when(F.col("is_concentrated"), 1).otherwise(0)).alias("concentrated_positions"),
    )
    # Portfolio return
    .withColumn(
        "portfolio_return_pct",
        F.when(F.col("total_aum") > 0, F.round(F.col("total_pnl") / F.col("total_aum") * 100, 4))
        .otherwise(F.lit(0)),
    )
    # AUM tier
    .withColumn(
        "aum_tier",
        F.when(F.col("total_aum") >= 100_000_000, "LARGE ($100M+)")
        .when(F.col("total_aum") >= 25_000_000, "MID ($25M-$100M)")
        .when(F.col("total_aum") >= 5_000_000, "SMALL ($5M-$25M)")
        .otherwise("MICRO (<$5M)"),
    )
    # Diversification score (higher = more diversified)
    .withColumn(
        "diversification_score",
        F.round(F.col("num_positions") * F.col("sectors") / F.greatest(F.col("max_position_weight"), F.lit(1)), 2),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sector Allocation per Portfolio

# COMMAND ----------

df_sector_allocation = (
    df_latest
    .filter(F.col("sector").isNotNull())
    .groupBy("portfolio_id", "as_of_date", "sector")
    .agg(
        F.round(F.sum("market_value"), 2).alias("sector_market_value"),
        F.count("*").alias("positions_in_sector"),
        F.round(F.sum("unrealized_pnl"), 2).alias("sector_unrealized_pnl"),
    )
)

# Calculate sector weight within portfolio
w = Window.partitionBy("portfolio_id", "as_of_date")
df_sector_allocation = df_sector_allocation.withColumn(
    "sector_weight_pct",
    F.round(F.col("sector_market_value") / F.sum("sector_market_value").over(w) * 100, 2),
).withColumn("_computed_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

(
    df_portfolio_summary.write
    .format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.portfolio_summary")
)

(
    df_sector_allocation.write
    .format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.portfolio_sector_allocation")
)

print("Portfolio summary written to gold layer")

# COMMAND ----------

spark.sql(f"""
    SELECT portfolio_id, aum_tier,
           ROUND(total_aum, 0) as aum,
           num_positions, sectors,
           ROUND(total_pnl, 0) as total_pnl,
           ROUND(portfolio_return_pct, 2) as return_pct,
           diversification_score
    FROM {CATALOG}.{GOLD}.portfolio_summary
    ORDER BY total_aum DESC
    LIMIT 20
""").display()
