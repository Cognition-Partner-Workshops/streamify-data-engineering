# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Risk Metrics
# MAGIC Portfolio and instrument-level risk analytics.
# MAGIC - Value at Risk (VaR) estimates
# MAGIC - Concentration risk
# MAGIC - Sector exposure analysis
# MAGIC - Trader activity risk scoring

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
df_market = spark.table(f"{CATALOG}.{SILVER}.market_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Portfolio VaR (Parametric - 95% confidence)
# MAGIC Simplified parametric VaR based on position market values and historical volatility.

# COMMAND ----------

# Get latest volatility per symbol
w_latest = Window.partitionBy("symbol").orderBy(F.col("trade_date").desc())

df_vol = (
    df_market
    .filter(F.col("volatility_20").isNotNull())
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .select("symbol", "volatility_20", "trade_date")
)

latest_date = df_positions.agg(F.max("as_of_date")).collect()[0][0]
df_latest_pos = df_positions.filter(F.col("as_of_date") == latest_date)

# Join positions with volatility
df_pos_risk = (
    df_latest_pos
    .join(df_vol, on="symbol", how="left")
    .withColumn("daily_vol", F.coalesce(F.col("volatility_20") / 100 * F.sqrt(F.lit(78)), F.lit(0.02)))
    # Individual position VaR (95% = 1.645 * sigma * value)
    .withColumn("position_var_95", F.round(1.645 * F.col("daily_vol") * F.abs(F.col("market_value")), 2))
)

# Portfolio-level VaR (simple sum — conservative, ignores correlations)
df_portfolio_var = (
    df_pos_risk
    .groupBy("portfolio_id", "as_of_date")
    .agg(
        F.round(F.sum("market_value"), 2).alias("total_aum"),
        F.round(F.sum("position_var_95"), 2).alias("portfolio_var_95"),
        F.count("*").alias("num_positions"),
        F.round(F.avg("daily_vol") * 100, 2).alias("avg_position_volatility_pct"),
        F.round(F.max("daily_vol") * 100, 2).alias("max_position_volatility_pct"),
    )
    .withColumn(
        "var_to_aum_pct",
        F.when(F.col("total_aum") > 0, F.round(F.col("portfolio_var_95") / F.col("total_aum") * 100, 4))
        .otherwise(F.lit(0)),
    )
    .withColumn(
        "risk_rating",
        F.when(F.col("var_to_aum_pct") > 5, "HIGH")
        .when(F.col("var_to_aum_pct") > 2, "MODERATE")
        .otherwise("LOW"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concentration Risk

# COMMAND ----------

df_concentration = (
    df_latest_pos
    .groupBy("portfolio_id", "as_of_date")
    .agg(
        # Herfindahl-Hirschman Index (HHI) — sum of squared weights
        F.round(F.sum(F.pow(F.col("weight") / 100, 2)), 6).alias("hhi"),
        F.round(F.max("weight"), 2).alias("top_position_weight_pct"),
        # Top-5 concentration
        F.count("*").alias("num_positions"),
    )
    .withColumn(
        "effective_num_positions",
        F.when(F.col("hhi") > 0, F.round(1.0 / F.col("hhi"), 1)).otherwise(F.col("num_positions")),
    )
    .withColumn(
        "concentration_risk",
        F.when(F.col("hhi") > 0.25, "HIGHLY_CONCENTRATED")
        .when(F.col("hhi") > 0.15, "MODERATELY_CONCENTRATED")
        .otherwise("DIVERSIFIED"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trader Risk Scoring

# COMMAND ----------

df_trader_risk = (
    df_trades
    .groupBy("trader_id")
    .agg(
        F.count("*").alias("total_trades"),
        F.round(F.sum("notional_value"), 2).alias("total_notional"),
        F.round(F.avg("notional_value"), 2).alias("avg_trade_size"),
        F.round(F.max("notional_value"), 2).alias("max_trade_size"),
        F.round(F.sum("commission") + F.sum("fees"), 2).alias("total_costs"),
        F.countDistinct("symbol").alias("instruments_traded"),
        F.countDistinct("trade_date").alias("active_days"),
        F.countDistinct("execution_venue").alias("venues_used"),
    )
    .withColumn("avg_daily_trades", F.round(F.col("total_trades") / F.greatest(F.col("active_days"), F.lit(1)), 1))
    .withColumn("avg_daily_notional", F.round(F.col("total_notional") / F.greatest(F.col("active_days"), F.lit(1)), 0))
    # Risk score: higher = more activity/risk
    .withColumn(
        "activity_risk_score",
        F.round(
            (F.col("avg_daily_notional") / 1_000_000) * (F.col("avg_daily_trades") / 10),
            2,
        ),
    )
    .withColumn(
        "trader_risk_tier",
        F.when(F.col("activity_risk_score") > 50, "HIGH_ACTIVITY")
        .when(F.col("activity_risk_score") > 10, "MODERATE_ACTIVITY")
        .otherwise("LOW_ACTIVITY"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

for df, name in [
    (df_portfolio_var, "portfolio_var"),
    (df_concentration, "concentration_risk"),
    (df_trader_risk, "trader_risk_scores"),
]:
    df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.{name}")

print("Risk metrics written to gold layer")

# COMMAND ----------

spark.sql(f"""
    SELECT portfolio_id, risk_rating,
           ROUND(total_aum, 0) as aum,
           ROUND(portfolio_var_95, 0) as var_95,
           ROUND(var_to_aum_pct, 2) as var_pct,
           ROUND(avg_position_volatility_pct, 2) as avg_vol_pct
    FROM {CATALOG}.{GOLD}.portfolio_var
    ORDER BY portfolio_var_95 DESC
    LIMIT 15
""").display()
