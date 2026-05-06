# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Transform Orders
# MAGIC Cleanse and enrich order flow data.
# MAGIC - Validate order integrity (price, quantity, status transitions)
# MAGIC - Enrich with instrument metadata
# MAGIC - Calculate fill rates and slippage

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "trading_platform"
BRONZE = "bronze"
SILVER = "silver"
TABLE = "orders"

# COMMAND ----------

df_orders = spark.table(f"{CATALOG}.{BRONZE}.{TABLE}")
df_instruments = spark.table(f"{CATALOG}.{SILVER}.instruments")

print(f"Bronze orders: {df_orders.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanse & Enrich

# COMMAND ----------

df_silver = (
    df_orders
    .drop("_ingested_at", "_source_file")
    .filter(
        F.col("order_id").isNotNull()
        & F.col("instrument_id").isNotNull()
        & (F.col("quantity") > 0)
    )
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    # Derived: order duration in seconds
    .withColumn(
        "order_duration_seconds",
        F.unix_timestamp("updated_at") - F.unix_timestamp("created_at"),
    )
    # Derived: fill rate
    .withColumn(
        "fill_rate",
        F.when(F.col("quantity") > 0, F.round(F.col("filled_quantity") / F.col("quantity"), 4))
        .otherwise(F.lit(0.0)),
    )
    # Derived: slippage (difference between requested and fill price)
    .withColumn(
        "slippage",
        F.when(
            F.col("avg_fill_price").isNotNull() & F.col("price").isNotNull(),
            F.round(F.col("avg_fill_price") - F.col("price"), 6),
        ).otherwise(F.lit(None)),
    )
    # Derived: slippage in basis points
    .withColumn(
        "slippage_bps",
        F.when(
            F.col("slippage").isNotNull() & (F.col("price") > 0),
            F.round(F.col("slippage") / F.col("price") * 10000, 2),
        ).otherwise(F.lit(None)),
    )
    # Derived: order date
    .withColumn("order_date", F.to_date("created_at"))
    # Derived: is terminal state
    .withColumn(
        "is_terminal",
        F.col("status").isin("FILLED", "CANCELLED", "REJECTED", "EXPIRED"),
    )
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
        ),
        on="instrument_id",
        how="left",
    )
    .withColumn("_processed_at", F.current_timestamp())
)

print(f"Silver orders: {df_enriched.count():,}")

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
        .merge(df_enriched.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_enriched.write.format("delta").mode("overwrite").partitionBy("order_date").saveAsTable(target_table)

print(f"Written to {target_table}")

# COMMAND ----------

spark.sql(f"""
    SELECT status, order_type,
           COUNT(*) as count,
           ROUND(AVG(fill_rate), 4) as avg_fill_rate,
           ROUND(AVG(slippage_bps), 2) as avg_slippage_bps
    FROM {target_table}
    GROUP BY status, order_type
    ORDER BY count DESC
""").display()
