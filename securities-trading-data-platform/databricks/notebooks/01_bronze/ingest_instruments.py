# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ingest Instruments
# MAGIC Load raw instrument reference data from S3 into Delta Lake.
# MAGIC This is a full-refresh load since instruments are reference/dimension data.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "trading_platform"
SCHEMA = "bronze"
TABLE = "instruments"
S3_PATH = "s3://securities-trading-data-platform-dev/bronze/instruments/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema

# COMMAND ----------

instruments_schema = StructType([
    StructField("instrument_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("name", StringType(), False),
    StructField("asset_class", StringType(), False),
    StructField("exchange", StringType(), False),
    StructField("currency", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("isin", StringType(), True),
    StructField("cusip", StringType(), True),
    StructField("lot_size", IntegerType(), True),
    StructField("tick_size", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("listed_date", DateType(), True),
    StructField("expiry_date", DateType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from S3

# COMMAND ----------

df_raw = (
    spark.read
    .format("parquet")
    .schema(instruments_schema)
    .load(S3_PATH)
)

# Add ingestion metadata
df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

print(f"Read {df_bronze.count()} instrument records from S3")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta (Full Refresh)

# COMMAND ----------

(
    df_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
)

print(f"Instruments written to {CATALOG}.{SCHEMA}.{TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------

spark.sql(f"SELECT asset_class, COUNT(*) as count FROM {CATALOG}.{SCHEMA}.{TABLE} GROUP BY asset_class ORDER BY count DESC").display()
