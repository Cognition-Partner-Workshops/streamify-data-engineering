"""
Test script for validating Kafka-to-Spark streaming connectivity.

This is a standalone test utility used to verify that the Spark Structured Streaming
pipeline can successfully connect to and consume messages from a Kafka broker. It uses
NYC Yellow Taxi ride data as a simple test dataset, outputting parsed records to the
console for visual verification.

Purpose:
    - Validate Kafka broker connectivity before deploying the main streaming pipeline
    - Test Spark's Kafka connector configuration and dependencies
    - Debug network/firewall issues between Spark and Kafka clusters

This script is NOT part of the production Streamify pipeline. It serves as a
diagnostic tool during initial setup and troubleshooting.

Environment Variables:
    KAFKA_ADDRESS: Hostname/IP of Kafka broker (default: 'localhost')

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 stream_taxi_json.py

Prerequisites:
    - Kafka broker running with 'yellow_taxi_ride.json' topic
    - Test data being produced to the topic (e.g., via produce_taxi_json.py)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, StructField, StructType
from pyspark.sql.functions import from_json, col

# Kafka broker address from environment or default to localhost for local testing
KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", 'localhost')

# Initialize Spark session for the test streaming application
spark = SparkSession \
    .builder \
    .appName("Stream Taxi Data") \
    .getOrCreate()

# Create a streaming DataFrame by subscribing to the test Kafka topic
taxi_rides = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{KAFKA_ADDRESS}:9092") \
    .option("subscribe", "yellow_taxi_ride.json") \
    .load()

# Extract the JSON payload from Kafka message value (key is ignored for this test)
taxi_rides = taxi_rides.selectExpr("CAST(value AS STRING)")

# Define schema for NYC Yellow Taxi ride data
# This schema matches the structure produced by the test data generator
taxi_schema = StructType([
    StructField("vendorId", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("pickup_location", IntegerType(), True),
    StructField("dropoff_location", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("pickup_datetime", TimestampType(), True)
])

# Parse JSON payload using the defined schema
taxi_rides = taxi_rides.select(from_json(col("value"), taxi_schema).alias("data")).select("data.*")

# Write parsed records to console for visual verification
# In production, this would be replaced with a file or database sink
taxi_rides \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
