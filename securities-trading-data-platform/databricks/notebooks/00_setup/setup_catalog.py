# Databricks notebook source
# MAGIC %md
# MAGIC # Catalog & Schema Setup
# MAGIC Initialize the Unity Catalog, schemas, and external locations for the
# MAGIC securities trading data platform.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG_NAME = "trading_platform"
S3_BUCKET = "securities-trading-data-platform-dev"
EXTERNAL_LOCATION = "securities-trading-data-platform-data-lake"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog & Schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {schema}
        COMMENT '{schema.title()} layer of the medallion architecture'
    """)

print(f"Catalog '{CATALOG_NAME}' and schemas created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume for Raw File Access (Optional)
# MAGIC Volumes provide direct file access to S3 paths through Unity Catalog.

# COMMAND ----------

for tier in ["bronze", "silver", "gold"]:
    spark.sql(f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS {CATALOG_NAME}.{tier}.raw_files
        LOCATION 's3://{S3_BUCKET}/{tier}/'
        COMMENT 'External volume for {tier} raw files'
    """)

print("Volumes created successfully.")
