# Databricks notebook source

# MAGIC %md
# MAGIC # Ingest Events via Auto Loader
# MAGIC
# MAGIC This notebook provides an **alternative ingestion path** using
# MAGIC Databricks Auto Loader (`cloudFiles`) instead of consuming directly
# MAGIC from Kafka.
# MAGIC
# MAGIC **When to use this notebook:**
# MAGIC - During migration, when events are still landing as Parquet files on
# MAGIC   cloud storage (GCS/S3/ADLS) from the legacy PySpark streaming job.
# MAGIC - When Kafka is not directly accessible from Databricks.
# MAGIC - For backfilling historical data that already exists as files.
# MAGIC
# MAGIC **How Auto Loader works:**
# MAGIC 1. Watches a cloud storage directory for new files.
# MAGIC 2. Tracks which files have been processed via a checkpoint.
# MAGIC 3. Incrementally loads only new files on each trigger.
# MAGIC 4. Writes to Delta Lake tables in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("source_base_path", "", "Cloud Storage Base Path (e.g. gs://bucket)")
dbutils.widgets.text("catalog", "streamify_catalog", "Unity Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema Name")
dbutils.widgets.text("checkpoint_base", "/Volumes/streamify_catalog/bronze/_checkpoints/autoloader", "Checkpoint Base Path")

SOURCE_BASE = dbutils.widgets.get("source_base_path")
CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ensure catalog and schema exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Import helper functions

# COMMAND ----------

# MAGIC %run ./streaming_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Define schemas
# MAGIC
# MAGIC Auto Loader can infer schemas, but providing explicit schemas avoids
# MAGIC type-inference surprises and matches the original pipeline's contract.

# COMMAND ----------

from pyspark.sql.types import (
    IntegerType,
    StringType,
    DoubleType,
    StructField,
    StructType,
    LongType,
    BooleanType,
    TimestampType,
)

# Schemas match the Parquet output of the original streaming job
# (ts is already a timestamp after processing, not epoch-ms)
autoloader_schemas = {
    "listen_events": StructType(
        [
            StructField("artist", StringType(), True),
            StructField("song", StringType(), True),
            StructField("duration", DoubleType(), True),
            StructField("ts", TimestampType(), True),
            StructField("sessionid", IntegerType(), True),
            StructField("auth", StringType(), True),
            StructField("level", StringType(), True),
            StructField("itemInSession", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("zip", IntegerType(), True),
            StructField("state", StringType(), True),
            StructField("userAgent", StringType(), True),
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True),
            StructField("userId", LongType(), True),
            StructField("lastName", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("registration", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("day", IntegerType(), True),
        ]
    ),
    "page_view_events": StructType(
        [
            StructField("ts", TimestampType(), True),
            StructField("sessionId", IntegerType(), True),
            StructField("page", StringType(), True),
            StructField("auth", StringType(), True),
            StructField("method", StringType(), True),
            StructField("status", IntegerType(), True),
            StructField("level", StringType(), True),
            StructField("itemInSession", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("zip", IntegerType(), True),
            StructField("state", StringType(), True),
            StructField("userAgent", StringType(), True),
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True),
            StructField("userId", IntegerType(), True),
            StructField("lastName", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("registration", LongType(), True),
            StructField("artist", StringType(), True),
            StructField("song", StringType(), True),
            StructField("duration", DoubleType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("day", IntegerType(), True),
        ]
    ),
    "auth_events": StructType(
        [
            StructField("ts", TimestampType(), True),
            StructField("sessionId", IntegerType(), True),
            StructField("level", StringType(), True),
            StructField("itemInSession", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("zip", IntegerType(), True),
            StructField("state", StringType(), True),
            StructField("userAgent", StringType(), True),
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True),
            StructField("userId", IntegerType(), True),
            StructField("lastName", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("registration", LongType(), True),
            StructField("success", BooleanType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("day", IntegerType(), True),
        ]
    ),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run Auto Loader for each event type
# MAGIC
# MAGIC `trigger(availableNow=True)` processes all available files, then
# MAGIC stops. This is ideal for scheduled Workflow runs.

# COMMAND ----------

TOPICS = ["listen_events", "page_view_events", "auth_events"]

for topic in TOPICS:
    source_path = f"{SOURCE_BASE}/{topic}"
    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{topic}"
    checkpoint = f"{CHECKPOINT_BASE}/{topic}"
    schema = autoloader_schemas[topic]

    print(f"Ingesting {topic} from {source_path} -> {target_table}")

    query = create_autoloader_stream(
        spark,
        source_path=source_path,
        source_format="parquet",
        schema=schema,
        target_table=target_table,
        checkpoint_path=checkpoint,
    )
    query.awaitTermination()
    print(f"  {topic} ingestion complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify row counts

# COMMAND ----------

for topic in TOPICS:
    count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{topic}").count()
    print(f"{topic}: {count} rows")
