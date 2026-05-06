# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Transform Trades
# MAGIC Cleanse, validate, and enrich trade execution data.
# MAGIC - Data quality validation (price > 0, quantity > 0, valid status)
# MAGIC - Enrich with instrument reference data
# MAGIC - Calculate derived metrics (effective cost, market impact estimate)
# MAGIC - Incremental processing via merge on trade_id

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
BRONZE = "bronze"
SILVER = "silver"
TABLE = "trades"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Trades + Silver Instruments

# COMMAND ----------

df_trades = spark.table(f"{CATALOG}.{BRONZE}.{TABLE}")
df_instruments = spark.table(f"{CATALOG}.{SILVER}.instruments")

print(f"Bronze trades: {df_trades.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality & Enrichment

# COMMAND ----------

df_silver = (
    df_trades
    .drop("_ingested_at", "_source_file")
    # Data quality: filter invalid records
    .filter(
        (F.col("price") > 0)
        & (F.col("quantity") > 0)
        & F.col("trade_id").isNotNull()
        & F.col("instrument_id").isNotNull()
        & F.col("status").isin("EXECUTED", "SETTLED", "FAILED", "CANCELLED")
    )
    # Parse timestamps
    .withColumn("executed_at", F.to_timestamp("executed_at"))
    .withColumn("trade_date", F.to_date("trade_date"))
    .withColumn("settlement_date", F.to_date("settlement_date"))
    # Derived: settlement days
    .withColumn(
        "settlement_days",
        F.datediff(F.col("settlement_date"), F.col("trade_date")),
    )
    # Derived: execution hour bucket
    .withColumn("execution_hour", F.hour("executed_at"))
    .withColumn(
        "session",
        F.when(F.col("execution_hour") < 10, "EARLY")
        .when(F.col("execution_hour") < 12, "MORNING")
        .when(F.col("execution_hour") < 14, "MIDDAY")
        .otherwise("AFTERNOON"),
    )
    # Derived: effective total cost
    .withColumn("total_cost", F.col("notional_value") + F.col("commission") + F.col("fees"))
    # Derived: cost basis per share
    .withColumn("cost_per_share", F.round(F.col("total_cost") / F.col("quantity"), 6))
    # Derived: is_settled flag
    .withColumn("is_settled", F.col("status") == "SETTLED")
)

# Enrich with instrument data
df_enriched = (
    df_silver
    .join(
        df_instruments.select(
            "instrument_id",
            F.col("name").alias("instrument_name"),
            F.col("asset_class"),
            F.col("sector"),
            F.col("instrument_type"),
        ),
        on="instrument_id",
        how="left",
    )
    .withColumn("_processed_at", F.current_timestamp())
)

print(f"Silver trades after enrichment: {df_enriched.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver (Merge)

# COMMAND ----------

target_table = f"{CATALOG}.{SILVER}.{TABLE}"

if spark.catalog.tableExists(target_table):
    from delta.tables import DeltaTable

    delta_target = DeltaTable.forName(spark, target_table)
    (
        delta_target.alias("t")
        .merge(df_enriched.alias("s"), "t.trade_id = s.trade_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged into {target_table}")
else:
    (
        df_enriched.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("trade_date")
        .saveAsTable(target_table)
    )
    print(f"Created {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------

spark.sql(f"""
    SELECT
        trade_date,
        COUNT(*) as trades,
        ROUND(SUM(notional_value), 2) as total_notional,
        ROUND(AVG(price), 2) as avg_price,
        ROUND(SUM(commission), 2) as total_commission
    FROM {target_table}
    WHERE status IN ('EXECUTED', 'SETTLED')
    GROUP BY trade_date
    ORDER BY trade_date DESC
    LIMIT 10
""").display()
