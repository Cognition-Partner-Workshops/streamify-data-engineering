# Databricks notebook source

# MAGIC %md
# MAGIC # Load Songs Reference Data
# MAGIC
# MAGIC Replaces `airflow/dags/load_songs_dag.py` which downloaded the songs
# MAGIC CSV, converted it to Parquet, uploaded to GCS, and created a BigQuery
# MAGIC external table.
# MAGIC
# MAGIC On Databricks we download the CSV and load it directly into a Delta
# MAGIC Lake table in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "streamify_catalog", "Unity Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema Name")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")

SONGS_URL = "https://github.com/ankurchavda/streamify/raw/main/dbt/seeds/songs.csv"
SONGS_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.songs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ensure catalog and schema exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Download songs CSV
# MAGIC
# MAGIC We use `%sh` to download the file to the driver node, then read it
# MAGIC with Spark.

# COMMAND ----------

import subprocess, os

local_path = "/tmp/songs.csv"
subprocess.run(["curl", "-sSLf", SONGS_URL, "-o", local_path], check=True)
assert os.path.exists(local_path), "Download failed"
print(f"Downloaded songs CSV to {local_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read CSV and write to Delta table

# COMMAND ----------

songs_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"file:{local_path}")
)

(
    songs_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SONGS_TABLE)
)

print(f"Loaded {songs_df.count()} songs into {SONGS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify

# COMMAND ----------

display(spark.table(SONGS_TABLE).limit(10))
