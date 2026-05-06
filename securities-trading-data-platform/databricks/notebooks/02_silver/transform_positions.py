# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Transform Positions
# MAGIC Cleanse position snapshots and enrich with instrument metadata.
# MAGIC - Validate P&L calculations
# MAGIC - Enrich with instrument reference data
# MAGIC - Add risk classification

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
BRONZE = "bronze"
SILVER = "silver"
TABLE = "positions"

# COMMAND ----------

df_positions = spark.table(f"{CATALOG}.{BRONZE}.{TABLE}")
df_instruments = spark.table(f"{CATALOG}.{SILVER}.instruments")

print(f"Bronze positions: {df_positions.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanse & Enrich

# COMMAND ----------

df_silver = (
    df_positions
    .drop("_ingested_at", "_source_file")
    .filter(
        F.col("position_id").isNotNull()
        & F.col("portfolio_id").isNotNull()
        & F.col("instrument_id").isNotNull()
        & (F.col("quantity") != 0)
    )
    .withColumn("as_of_date", F.to_date("as_of_date"))
    .withColumn("last_updated", F.to_timestamp("last_updated"))
    # Recalculate P&L for consistency
    .withColumn("calc_unrealized_pnl", F.round((F.col("current_price") - F.col("avg_cost")) * F.col("quantity"), 2))
    .withColumn("calc_market_value", F.round(F.col("current_price") * F.col("quantity"), 2))
    # P&L percentage
    .withColumn(
        "unrealized_pnl_pct",
        F.when(
            F.col("avg_cost") > 0,
            F.round((F.col("current_price") - F.col("avg_cost")) / F.col("avg_cost") * 100, 2),
        ).otherwise(F.lit(0.0)),
    )
    # Risk classification based on P&L
    .withColumn(
        "pnl_risk_class",
        F.when(F.col("unrealized_pnl_pct") < -10, "HIGH_LOSS")
        .when(F.col("unrealized_pnl_pct") < -5, "MODERATE_LOSS")
        .when(F.col("unrealized_pnl_pct") < 0, "SLIGHT_LOSS")
        .when(F.col("unrealized_pnl_pct") < 5, "SLIGHT_GAIN")
        .when(F.col("unrealized_pnl_pct") < 10, "MODERATE_GAIN")
        .otherwise("HIGH_GAIN"),
    )
    # Position direction
    .withColumn(
        "direction",
        F.when(F.col("quantity") > 0, "LONG").otherwise("SHORT"),
    )
    # Concentration flag (weight > 10% of portfolio)
    .withColumn("is_concentrated", F.col("weight") > 10.0)
)

# Enrich with instrument data
df_enriched = (
    df_silver
    .join(
        df_instruments.select(
            "instrument_id",
            F.col("name").alias("instrument_name"),
            "asset_class",
            "sector",
            "instrument_type",
        ),
        on="instrument_id",
        how="left",
    )
    .withColumn("_processed_at", F.current_timestamp())
)

print(f"Silver positions: {df_enriched.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver

# COMMAND ----------

target_table = f"{CATALOG}.{SILVER}.{TABLE}"

if spark.catalog.tableExists(target_table):
    from delta.tables import DeltaTable

    delta_target = DeltaTable.forName(spark, target_table)
    (
        delta_target.alias("t")
        .merge(df_enriched.alias("s"), "t.position_id = s.position_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_enriched.write.format("delta").mode("overwrite").partitionBy("as_of_date").saveAsTable(target_table)

print(f"Written to {target_table}")

# COMMAND ----------

spark.sql(f"""
    SELECT as_of_date, pnl_risk_class,
           COUNT(*) as positions,
           ROUND(SUM(market_value), 2) as total_market_value,
           ROUND(SUM(unrealized_pnl), 2) as total_unrealized_pnl
    FROM {target_table}
    WHERE as_of_date = (SELECT MAX(as_of_date) FROM {target_table})
    GROUP BY as_of_date, pnl_risk_class
    ORDER BY total_market_value DESC
""").display()
