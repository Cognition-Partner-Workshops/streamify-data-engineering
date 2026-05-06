# Databricks notebook source

# MAGIC %md
# MAGIC # Streaming Helper Functions
# MAGIC
# MAGIC This notebook replaces `spark_streaming/streaming_functions.py`.
# MAGIC
# MAGIC **Key changes from the original:**
# MAGIC | Aspect | Original | Databricks |
# MAGIC |---|---|---|
# MAGIC | Spark session | Created manually with `SparkSession.builder` | Already provided by the Databricks runtime (`spark`) |
# MAGIC | Write sink | Parquet files on GCS, partitioned by month/day/hour | Delta Lake managed tables in Unity Catalog |
# MAGIC | Checkpoint | GCS path | DBFS or Unity Catalog Volumes path |
# MAGIC | Trigger | `processingTime="120 seconds"` | `availableNow=True` for batch-style or `processingTime` for continuous |

# COMMAND ----------

from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, year, udf

# COMMAND ----------

# MAGIC %md
# MAGIC ### String decoding UDF
# MAGIC
# MAGIC The original pipeline had a UDF to fix double-encoded unicode strings
# MAGIC coming from eventsim. We keep the same logic here.

# COMMAND ----------

@udf
def string_decode(s, encoding="utf-8"):
    """Decode eventsim's double-encoded unicode strings."""
    if s:
        return (
            s.encode("latin1")
            .decode("unicode-escape")
            .encode("latin1")
            .decode(encoding)
            .strip('"')
        )
    return s

# COMMAND ----------

# MAGIC %md
# MAGIC ### create_kafka_read_stream
# MAGIC
# MAGIC Reads from a Kafka topic. On Databricks the Kafka connector is
# MAGIC pre-installed, so no extra `--packages` flag is needed.

# COMMAND ----------

def create_kafka_read_stream(spark, kafka_bootstrap_servers, topic, starting_offset="earliest"):
    """
    Create a Kafka read stream.

    Parameters
    ----------
    spark : SparkSession
    kafka_bootstrap_servers : str
        host:port (e.g. "broker1:9092,broker2:9092")
    topic : str
    starting_offset : str
        "earliest" or "latest"

    Returns
    -------
    DataFrame  (streaming)
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("failOnDataLoss", False)
        .option("startingOffsets", starting_offset)
        .option("subscribe", topic)
        .load()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### process_stream
# MAGIC
# MAGIC Parses the Kafka value JSON, converts the epoch-ms timestamp to a
# MAGIC proper `TimestampType`, and adds calendar partition columns.

# COMMAND ----------

def process_stream(stream, stream_schema, topic):
    """
    Parse Kafka JSON value and enrich with time-partition columns.

    Parameters
    ----------
    stream : DataFrame (streaming)
    stream_schema : StructType
    topic : str

    Returns
    -------
    DataFrame (streaming)
    """
    stream = (
        stream
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), stream_schema).alias("data"))
        .select("data.*")
    )

    stream = (
        stream
        .withColumn("ts", (col("ts") / 1000).cast("timestamp"))
        .withColumn("year", year(col("ts")))
        .withColumn("month", month(col("ts")))
        .withColumn("hour", hour(col("ts")))
        .withColumn("day", dayofmonth(col("ts")))
    )

    if topic in ("listen_events", "page_view_events"):
        stream = (
            stream
            .withColumn("song", string_decode("song"))
            .withColumn("artist", string_decode("artist"))
        )

    return stream

# COMMAND ----------

# MAGIC %md
# MAGIC ### create_delta_write_stream
# MAGIC
# MAGIC Replaces the original `create_file_write_stream` that wrote Parquet
# MAGIC files to GCS. Now writes to a Delta Lake table in Unity Catalog.
# MAGIC
# MAGIC The trigger interval is kept at 120 seconds to match the original
# MAGIC pipeline cadence.

# COMMAND ----------

def create_delta_write_stream(
    stream,
    target_table,
    checkpoint_path,
    trigger="120 seconds",
    output_mode="append",
):
    """
    Write a streaming DataFrame to a Delta Lake table.

    Parameters
    ----------
    stream : DataFrame (streaming)
    target_table : str
        Fully-qualified Unity Catalog table name
        (e.g. "streamify_catalog.bronze.listen_events").
    checkpoint_path : str
    trigger : str
        Processing time interval.
    output_mode : str
        "append", "complete", or "update".

    Returns
    -------
    DataStreamWriter (call `.start()` to begin)
    """
    return (
        stream.writeStream
        .format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime=trigger)
        .toTable(target_table)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### create_autoloader_stream  (new)
# MAGIC
# MAGIC Auto Loader (`cloudFiles`) provides incremental file ingestion from
# MAGIC cloud storage.  This is the recommended approach when the upstream
# MAGIC system lands files (e.g. Parquet from an existing Kafka→GCS leg)
# MAGIC rather than exposing a Kafka endpoint directly.

# COMMAND ----------

def create_autoloader_stream(
    spark,
    source_path,
    source_format,
    schema,
    target_table,
    checkpoint_path,
):
    """
    Incrementally ingest files from cloud storage using Auto Loader.

    Parameters
    ----------
    spark : SparkSession
    source_path : str
        Cloud storage path (e.g. "gs://bucket/listen_events/").
    source_format : str
        "parquet", "json", "csv", etc.
    schema : StructType
    target_table : str
        Fully-qualified Delta table name.
    checkpoint_path : str

    Returns
    -------
    DataStreamWriter (call `.start()` to begin)
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .schema(schema)
        .load(source_path)
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
