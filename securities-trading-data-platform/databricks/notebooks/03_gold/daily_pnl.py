# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Daily P&L Report
# MAGIC Aggregate daily profit and loss across portfolios and traders.
# MAGIC Combines trade execution data with position snapshots for comprehensive P&L.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
SILVER = "silver"
GOLD = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Realized P&L from Trades

# COMMAND ----------

df_trades = spark.table(f"{CATALOG}.{SILVER}.trades")

df_realized_pnl = (
    df_trades
    .filter(F.col("status").isin("EXECUTED", "SETTLED"))
    .groupBy("trade_date", "portfolio_id", "trader_id", "symbol", "sector", "asset_class")
    .agg(
        F.count("*").alias("num_trades"),
        F.sum(F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(0)).alias("buy_quantity"),
        F.sum(F.when(F.col("side") == "SELL", F.col("quantity")).otherwise(0)).alias("sell_quantity"),
        F.round(F.sum("notional_value"), 2).alias("total_notional"),
        F.round(F.sum("commission"), 2).alias("total_commission"),
        F.round(F.sum("fees"), 2).alias("total_fees"),
        F.round(F.sum("net_amount"), 2).alias("net_cash_flow"),
        F.round(F.avg("price"), 4).alias("avg_trade_price"),
        F.round(F.avg("cost_per_share"), 6).alias("avg_cost_per_share"),
    )
    .withColumn("total_trading_costs", F.round(F.col("total_commission") + F.col("total_fees"), 2))
    .withColumn("net_quantity", F.col("buy_quantity") - F.col("sell_quantity"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unrealized P&L from Positions

# COMMAND ----------

df_positions = spark.table(f"{CATALOG}.{SILVER}.positions")

df_unrealized_pnl = (
    df_positions
    .groupBy("as_of_date", "portfolio_id", "sector", "asset_class")
    .agg(
        F.count("*").alias("num_positions"),
        F.round(F.sum("market_value"), 2).alias("total_market_value"),
        F.round(F.sum("unrealized_pnl"), 2).alias("total_unrealized_pnl"),
        F.round(F.sum("realized_pnl"), 2).alias("total_realized_pnl"),
        F.round(F.sum("total_pnl"), 2).alias("total_pnl"),
        F.round(F.avg("unrealized_pnl_pct"), 2).alias("avg_unrealized_pnl_pct"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combined Daily P&L

# COMMAND ----------

# Portfolio-level daily P&L from trades
df_daily_pnl = (
    df_realized_pnl
    .groupBy("trade_date", "portfolio_id")
    .agg(
        F.sum("num_trades").alias("total_trades"),
        F.round(F.sum("total_notional"), 2).alias("gross_notional"),
        F.round(F.sum("net_cash_flow"), 2).alias("net_cash_flow"),
        F.round(F.sum("total_trading_costs"), 2).alias("trading_costs"),
        F.sum("buy_quantity").alias("total_buy_qty"),
        F.sum("sell_quantity").alias("total_sell_qty"),
        F.countDistinct("symbol").alias("instruments_traded"),
    )
    .withColumn(
        "cost_to_notional_bps",
        F.when(F.col("gross_notional") > 0, F.round(F.col("trading_costs") / F.col("gross_notional") * 10000, 2))
        .otherwise(F.lit(0)),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

print(f"Daily P&L records: {df_daily_pnl.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

target_table = f"{CATALOG}.{GOLD}.daily_pnl"

(
    df_daily_pnl.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("trade_date")
    .saveAsTable(target_table)
)

# Also write the position-level unrealized P&L
unrealized_table = f"{CATALOG}.{GOLD}.unrealized_pnl"
(
    df_unrealized_pnl.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("as_of_date")
    .saveAsTable(unrealized_table)
)

print(f"Written to {target_table} and {unrealized_table}")

# COMMAND ----------

spark.sql(f"""
    SELECT trade_date, COUNT(DISTINCT portfolio_id) as portfolios,
           SUM(total_trades) as trades,
           ROUND(SUM(gross_notional), 0) as gross_notional,
           ROUND(SUM(net_cash_flow), 0) as net_cash_flow,
           ROUND(SUM(trading_costs), 0) as costs
    FROM {target_table}
    GROUP BY trade_date
    ORDER BY trade_date DESC
    LIMIT 20
""").display()
