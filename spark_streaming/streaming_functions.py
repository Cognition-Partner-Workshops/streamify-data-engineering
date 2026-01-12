"""
Streaming utility functions for the Streamify data pipeline.

This module provides reusable functions for building Spark Structured Streaming
applications that consume events from Kafka and write them to cloud storage.
The functions handle the complete streaming lifecycle including session management,
Kafka consumption, stream transformation, and file output.

Key Functions:
    - create_or_get_spark_session: Initialize Spark with appropriate configuration
    - create_kafka_read_stream: Set up Kafka consumer as a streaming source
    - process_stream: Transform raw Kafka messages into structured, partitioned data
    - create_file_write_stream: Configure output sink for writing to cloud storage

The module also includes a string_decode UDF to handle encoding issues in
artist and song names that may contain special characters.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, col, year, udf


@udf
def string_decode(s, encoding='utf-8'):
    """
    Decode strings with escaped unicode characters from Eventsim.

    Eventsim generates artist and song names that may contain special characters
    (accents, non-ASCII symbols) encoded as octal escape sequences. This UDF
    performs a multi-step decoding process to convert these escaped strings
    back to proper UTF-8 text.

    The decoding process:
        1. Encode to latin1 bytes (required for unicode-escape decoder)
        2. Decode unicode escape sequences (e.g., \\303\\251 -> é)
        3. Encode back to latin1 bytes (1:1 byte mapping)
        4. Decode to final UTF-8 string
        5. Strip any surrounding quotation marks

    Args:
        s: Input string potentially containing escaped unicode characters
        encoding: Target encoding for final output (default: 'utf-8')

    Returns:
        Decoded string with proper unicode characters, or None if input is None

    Example:
        Input:  "Bj\\303\\266rk" (escaped)
        Output: "Björk" (proper unicode)
    """
    if s:
        return (s.encode('latin1')
                .decode('unicode-escape')
                .encode('latin1')
                .decode(encoding)
                .strip('\"'))
    else:
        return s

def create_or_get_spark_session(app_name, master="yarn"):
    """
    Create a new SparkSession or retrieve an existing one.

    This function initializes a SparkSession configured for the streaming application.
    If a session with the same configuration already exists, it returns that session
    instead of creating a new one (singleton pattern).

    Args:
        app_name: Name of the Spark application, visible in the Spark UI
                  and cluster resource manager (e.g., "Eventsim Stream")
        master: Spark master URL specifying the cluster manager.
                Common values:
                - "yarn": Run on YARN cluster (default, used in production)
                - "local[*]": Run locally with all available cores
                - "spark://host:port": Connect to standalone Spark cluster

    Returns:
        SparkSession: Configured Spark session ready for streaming operations
    """
    spark = (SparkSession
             .builder
             .appName(app_name)
             .master(master=master)
             .getOrCreate())

    return spark


def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset="earliest"):
    """
    Create a Spark Structured Streaming reader for a Kafka topic.

    Configures a DataStreamReader to consume messages from a specified Kafka topic.
    The reader is set up with fault-tolerant options to handle data loss scenarios
    gracefully without failing the streaming job.

    Args:
        spark: Active SparkSession instance
        kafka_address: Hostname or IP address of the Kafka bootstrap server
                       (e.g., "localhost" or the external IP of the Kafka VM)
        kafka_port: Port number for the Kafka broker (typically "9092")
        topic: Name of the Kafka topic to subscribe to. In Streamify, this is
               one of: "listen_events", "page_view_events", or "auth_events"
        starting_offset: Where to start reading when no checkpoint exists.
                         Options: "earliest" (from beginning), "latest" (new only)

    Returns:
        DataFrame: A streaming DataFrame with Kafka message structure containing
                   columns: key, value, topic, partition, offset, timestamp, etc.
                   The 'value' column contains the JSON event payload.

    Note:
        The 'failOnDataLoss' option is set to False to prevent job failures when
        Kafka data is deleted due to retention policies or topic compaction.
    """
    read_stream = (spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port}")
                   .option("failOnDataLoss", False)
                   .option("startingOffsets", starting_offset)
                   .option("subscribe", topic)
                   .load())

    return read_stream


def process_stream(stream, stream_schema, topic):
    """
    Transform raw Kafka messages into structured, partitioned event data.

    This function performs three key transformations on the incoming stream:
    1. Schema Application: Parses JSON payloads from Kafka 'value' field using
       the provided StructType schema
    2. Timestamp Processing: Converts Unix epoch milliseconds to Spark timestamp
       and derives temporal partition columns (year, month, day, hour)
    3. String Encoding Fix: Decodes escaped unicode characters in artist/song
       names for listen_events and page_view_events topics

    The derived partition columns enable efficient time-based data organization
    in the output file structure (e.g., month=1/day=15/hour=10/).

    Args:
        stream: Streaming DataFrame from Kafka containing raw message data
        stream_schema: StructType schema defining the expected JSON structure
                       (from schema.py for the corresponding event type)
        topic: Name of the Kafka topic being processed. Used to determine
               whether string encoding fixes should be applied.

    Returns:
        DataFrame: Transformed streaming DataFrame with:
            - All fields from the original schema
            - 'ts' converted from milliseconds to timestamp
            - Added columns: year, month, day, hour (for partitioning)
            - Decoded artist/song names (for listen_events, page_view_events)
    """
    # Parse JSON payload from Kafka message value using the provided schema
    stream = (stream
              .selectExpr("CAST(value AS STRING)")
              .select(
                  from_json(col("value"), stream_schema).alias(
                      "data")
              )
              .select("data.*")
              )

    # Convert timestamp from milliseconds to Spark timestamp type and
    # derive temporal columns for partitioning the output files
    stream = (stream
              .withColumn("ts", (col("ts")/1000).cast("timestamp"))
              .withColumn("year", year(col("ts")))
              .withColumn("month", month(col("ts")))
              .withColumn("hour", hour(col("ts")))
              .withColumn("day", dayofmonth(col("ts")))
              )

    # Fix unicode encoding issues in artist and song names for relevant topics
    if topic in ["listen_events", "page_view_events"]:
        stream = (stream
                .withColumn("song", string_decode("song"))
                .withColumn("artist", string_decode("artist")) 
                )

    return stream


def create_file_write_stream(stream, storage_path, checkpoint_path, trigger="120 seconds", output_mode="append", file_format="parquet"):
    """
    Configure a streaming writer to output data to cloud storage.

    Creates a DataStreamWriter that writes processed events to a file-based sink
    (typically GCS) in a partitioned directory structure. The output is organized
    by month/day/hour to enable efficient time-based queries and data lifecycle
    management.

    Output Directory Structure:
        {storage_path}/
            month=1/
                day=15/
                    hour=10/
                        part-00000-*.parquet
                        part-00001-*.parquet

    Args:
        stream: Transformed streaming DataFrame ready for output
        storage_path: Base path for output files (e.g., "gs://bucket/listen_events")
        checkpoint_path: Location for Spark checkpoints to track processing progress.
                         Enables exactly-once semantics and recovery from failures.
        trigger: Processing interval determining how often micro-batches are written.
                 Default "120 seconds" balances latency vs. file count.
        output_mode: How to handle output records:
                     - "append": Add new records only (default, for streaming)
                     - "complete": Rewrite entire result (for aggregations)
                     - "update": Update changed records only
        file_format: Output file format. Default "parquet" provides efficient
                     columnar storage with compression.

    Returns:
        DataStreamWriter: Configured writer ready to be started with .start()

    Note:
        The partitionBy("month", "day", "hour") creates a Hive-style partitioned
        directory structure that enables partition pruning in downstream queries.
    """
    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    .partitionBy("month", "day", "hour")
                    .option("path", storage_path)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream
