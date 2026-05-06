# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Transform Instruments
# MAGIC Cleanse and standardize instrument reference data.
# MAGIC - Validate required fields and data types
# MAGIC - Standardize naming conventions
# MAGIC - Add derived classifications
# MAGIC - SCD Type 1 (overwrite) since instruments are slowly changing dimensions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

CATALOG = "trading_platform"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
TABLE = "instruments"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze

# COMMAND ----------

df_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE}")
print(f"Bronze records: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks & Transformations

# COMMAND ----------

df_silver = (
    df_bronze
    # Remove ingestion metadata columns
    .drop("_ingested_at", "_source_file")
    # Standardize string fields
    .withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
    .withColumn("name", F.trim(F.col("name")))
    .withColumn("asset_class", F.upper(F.trim(F.col("asset_class"))))
    .withColumn("exchange", F.upper(F.trim(F.col("exchange"))))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    # Validate: filter out records with missing required fields
    .filter(
        F.col("instrument_id").isNotNull()
        & F.col("symbol").isNotNull()
        & F.col("asset_class").isNotNull()
        & F.col("exchange").isNotNull()
    )
    # Derived: market capitalization tier (will be enriched later with actual data)
    .withColumn(
        "instrument_type",
        F.when(F.col("asset_class") == "EQUITY", "Stock")
        .when(F.col("asset_class") == "ETF", "Exchange Traded Fund")
        .when(F.col("asset_class") == "FIXED_INCOME", "Bond")
        .when(F.col("asset_class") == "OPTION", "Option")
        .when(F.col("asset_class") == "FUTURES", "Future")
        .otherwise("Other"),
    )
    # Derived: is_derivative flag
    .withColumn(
        "is_derivative",
        F.col("asset_class").isin("OPTION", "FUTURES"),
    )
    # Derived: has_expiry flag
    .withColumn("has_expiry", F.col("expiry_date").isNotNull())
    # Add processing timestamp
    .withColumn("_processed_at", F.current_timestamp())
)

# Deduplicate by instrument_id (keep latest)
window = Window.partitionBy("instrument_id").orderBy(F.col("_processed_at").desc())
df_silver = (
    df_silver
    .withColumn("_row_num", F.row_number().over(window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

print(f"Silver records after cleansing: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver (Merge/Upsert)

# COMMAND ----------

target_table = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE}"

# First run: create table; subsequent runs: merge
if spark.catalog.tableExists(target_table):
    from delta.tables import DeltaTable

    delta_target = DeltaTable.forName(spark, target_table)

    (
        delta_target.alias("target")
        .merge(df_silver.alias("source"), "target.instrument_id = source.instrument_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged into {target_table}")
else:
    df_silver.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"Created {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------

spark.sql(f"""
    SELECT asset_class, instrument_type, is_derivative, COUNT(*) as count
    FROM {target_table}
    GROUP BY asset_class, instrument_type, is_derivative
    ORDER BY count DESC
""").display()
