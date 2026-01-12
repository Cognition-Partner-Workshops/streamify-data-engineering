"""
Main Spark Streaming application for processing Eventsim music streaming events.

This script is the entry point for the real-time data pipeline that consumes events
from Kafka and writes them to Google Cloud Storage (GCS) in Parquet format. It
processes three concurrent event streams in parallel:

    1. listen_events: Song play events capturing user listening activity
    2. page_view_events: Website navigation events tracking page visits
    3. auth_events: Authentication events recording login/logout activity

Architecture:
    Kafka Topics --> Spark Structured Streaming --> GCS (Parquet files)

The pipeline runs continuously, writing micro-batches every 2 minutes (120 seconds)
to GCS. Data is partitioned by month/day/hour for efficient downstream processing
by Airflow and dbt.

Environment Variables:
    KAFKA_ADDRESS: Hostname/IP of Kafka broker (default: 'localhost')
    GCP_GCS_BUCKET: GCS bucket name for output (default: 'streamify')

Usage:
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \\
        stream_all_events.py

Output Structure:
    gs://{GCP_GCS_BUCKET}/
        listen_events/month=M/day=D/hour=H/*.parquet
        page_view_events/month=M/day=D/hour=H/*.parquet
        auth_events/month=M/day=D/hour=H/*.parquet
"""

import os
from streaming_functions import create_or_get_spark_session, create_kafka_read_stream, process_stream, create_file_write_stream
from schema import schema

# Kafka topic names corresponding to Eventsim event types
LISTEN_EVENTS_TOPIC = "listen_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
AUTH_EVENTS_TOPIC = "auth_events"

# Kafka broker configuration
KAFKA_PORT = "9092"

# Configuration from environment variables with sensible defaults
KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", 'localhost')
GCP_GCS_BUCKET = os.getenv("GCP_GCS_BUCKET", 'streamify')
GCS_STORAGE_PATH = f'gs://{GCP_GCS_BUCKET}'

# Initialize Spark session for streaming application
spark = create_or_get_spark_session('Eventsim Stream')
# Reset any previously terminated streams to allow clean restart
spark.streams.resetTerminated()

# =============================================================================
# STREAM SETUP: Create and configure read streams for each event type
# Each stream connects to its Kafka topic and applies the appropriate schema
# =============================================================================

# Listen events stream - captures song play activity (most critical for analytics)
listen_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, LISTEN_EVENTS_TOPIC)
listen_events = process_stream(
    listen_events, schema[LISTEN_EVENTS_TOPIC], LISTEN_EVENTS_TOPIC)

# Page view events stream - tracks website navigation and user engagement
page_view_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, PAGE_VIEW_EVENTS_TOPIC)
page_view_events = process_stream(
    page_view_events, schema[PAGE_VIEW_EVENTS_TOPIC], PAGE_VIEW_EVENTS_TOPIC)

# Auth events stream - records login/logout activity for user session analysis
auth_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, AUTH_EVENTS_TOPIC)
auth_events = process_stream(
    auth_events, schema[AUTH_EVENTS_TOPIC], AUTH_EVENTS_TOPIC)

# =============================================================================
# WRITE STREAM SETUP: Configure output writers for each event type
# Each writer outputs to GCS with separate checkpoint locations for fault tolerance
# Micro-batches are written every 2 minutes (120 seconds) in Parquet format
# =============================================================================

listen_events_writer = create_file_write_stream(listen_events,
                                                f"{GCS_STORAGE_PATH}/{LISTEN_EVENTS_TOPIC}",
                                                f"{GCS_STORAGE_PATH}/checkpoint/{LISTEN_EVENTS_TOPIC}"
                                                )

page_view_events_writer = create_file_write_stream(page_view_events,
                                                   f"{GCS_STORAGE_PATH}/{PAGE_VIEW_EVENTS_TOPIC}",
                                                   f"{GCS_STORAGE_PATH}/checkpoint/{PAGE_VIEW_EVENTS_TOPIC}"
                                                   )

auth_events_writer = create_file_write_stream(auth_events,
                                              f"{GCS_STORAGE_PATH}/{AUTH_EVENTS_TOPIC}",
                                              f"{GCS_STORAGE_PATH}/checkpoint/{AUTH_EVENTS_TOPIC}"
                                              )

# =============================================================================
# START STREAMING: Launch all three streams concurrently
# The streams run in parallel, each processing its respective Kafka topic
# =============================================================================

listen_events_writer.start()
auth_events_writer.start()
page_view_events_writer.start()

# Block until any stream terminates (due to error or manual shutdown)
# In production, this keeps the application running indefinitely
spark.streams.awaitAnyTermination()
