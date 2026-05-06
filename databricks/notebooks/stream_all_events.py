# Databricks notebook source

# MAGIC %md
# MAGIC # Streamify - Stream All Events to Delta Lake
# MAGIC
# MAGIC This notebook replaces the original PySpark Structured Streaming job
# MAGIC (`spark_streaming/stream_all_events.py`) that consumed Kafka topics and
# MAGIC wrote Parquet files to GCS.
# MAGIC
# MAGIC **What changed:**
# MAGIC | Original (GCP) | Databricks Lakehouse |
# MAGIC |---|---|
# MAGIC | Kafka → PySpark → Parquet on GCS | Kafka → Databricks Structured Streaming → Delta Lake tables |
# MAGIC | Manual schema enforcement | Auto Loader (`cloudFiles`) available for file-based ingestion; Kafka source retained for real-time path |
# MAGIC | Partitioned Parquet files | Delta Lake with optimized Z-ORDER and liquid clustering |
# MAGIC | Checkpoint on GCS | Checkpoint on DBFS / Unity Catalog managed storage |
# MAGIC
# MAGIC **Unity Catalog layout:**
# MAGIC ```
# MAGIC streamify_catalog
# MAGIC   └── bronze
# MAGIC       ├── listen_events
# MAGIC       ├── page_view_events
# MAGIC       └── auth_events
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC Set up the Kafka connection parameters and Delta Lake target paths.
# MAGIC Use Databricks widgets so that the notebook can be parameterised from
# MAGIC a Databricks Workflow.

# COMMAND ----------

dbutils.widgets.text("kafka_bootstrap_servers", "", "Kafka Bootstrap Servers")
dbutils.widgets.text("catalog", "streamify_catalog", "Unity Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema Name")
dbutils.widgets.text("checkpoint_base", "/Volumes/streamify_catalog/bronze/_checkpoints", "Checkpoint Base Path")

KAFKA_BOOTSTRAP_SERVERS = dbutils.widgets.get("kafka_bootstrap_servers")
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
# MAGIC ## 3. Import helpers
# MAGIC
# MAGIC The `streaming_functions` notebook is imported via `%run` so that its
# MAGIC functions are available in this notebook's scope.

# COMMAND ----------

# MAGIC %run ./streaming_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Define event schemas
# MAGIC
# MAGIC Identical to the original `spark_streaming/schema.py`, declared inline
# MAGIC so the notebook is self-contained.

# COMMAND ----------

from pyspark.sql.types import (
    IntegerType,
    StringType,
    DoubleType,
    StructField,
    StructType,
    LongType,
    BooleanType,
)

event_schemas = {
    "listen_events": StructType(
        [
            StructField("artist", StringType(), True),
            StructField("song", StringType(), True),
            StructField("duration", DoubleType(), True),
            StructField("ts", LongType(), True),
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
        ]
    ),
    "page_view_events": StructType(
        [
            StructField("ts", LongType(), True),
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
        ]
    ),
    "auth_events": StructType(
        [
            StructField("ts", LongType(), True),
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
        ]
    ),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Define topics to process

# COMMAND ----------

TOPICS = ["listen_events", "page_view_events", "auth_events"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create and start streaming queries
# MAGIC
# MAGIC For each topic we:
# MAGIC 1. Read from Kafka with `create_kafka_read_stream`
# MAGIC 2. Parse JSON and add time-partition columns with `process_stream`
# MAGIC 3. Write to a Delta Lake table in the bronze schema with `create_delta_write_stream`

# COMMAND ----------

writers = []
for topic in TOPICS:
    raw_stream = create_kafka_read_stream(spark, KAFKA_BOOTSTRAP_SERVERS, topic)
    processed = process_stream(raw_stream, event_schemas[topic], topic)

    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{topic}"
    checkpoint = f"{CHECKPOINT_BASE}/{topic}"

    query = create_delta_write_stream(processed, target_table, checkpoint)
    writers.append(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Await termination
# MAGIC
# MAGIC Keep the notebook alive until all streams are stopped (e.g. via the
# MAGIC Databricks Workflow cancel or manual stop).

# COMMAND ----------

for w in writers:
    w.awaitTermination()
