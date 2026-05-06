# Streamify — GCP to Databricks Lakehouse Migration Plan

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Comparison](#architecture-comparison)
3. [Stream Processing: PySpark → Databricks Notebooks](#stream-processing)
4. [Orchestration: Airflow → Databricks Workflows](#orchestration)
5. [Transformation: dbt-bigquery → dbt-databricks](#transformation)
6. [Unity Catalog Schema Layout](#unity-catalog-schema-layout)
7. [Delta Lake Partitioning Strategy](#delta-lake-partitioning-strategy)
8. [Auto Loader vs Structured Streaming](#auto-loader-vs-structured-streaming)
9. [Workflow Scheduling Configuration](#workflow-scheduling)
10. [Migration Steps](#migration-steps)
11. [Rollback Plan](#rollback-plan)

---

## Executive Summary

This document describes the migration of the **Streamify** data pipeline from a GCP-native stack (Kafka → PySpark on YARN → Parquet on GCS → Airflow → BigQuery → dbt-bigquery) to the **Databricks Lakehouse** platform (Kafka → Databricks Structured Streaming → Delta Lake → Databricks Workflows → dbt-databricks).

The migration consolidates compute, storage, orchestration, and governance onto a single platform while preserving the pipeline's star-schema output (dim_users, dim_songs, dim_artists, dim_location, dim_datetime, fact_streams).

---

## Architecture Comparison

| Layer | Original (GCP) | Databricks Lakehouse |
|---|---|---|
| **Ingestion** | PySpark Structured Streaming on YARN, reading Kafka, writing Parquet to GCS | Databricks notebook with Structured Streaming, writing to Delta Lake tables |
| **File format** | Parquet on GCS | Delta Lake (Parquet + transaction log) on cloud storage |
| **Batch loading** | Airflow tasks: create BigQuery external table → INSERT INTO staging → delete external table | Auto Loader (`cloudFiles`) for incremental file ingestion, or direct Kafka streaming |
| **Orchestration** | Apache Airflow (self-managed on GCE) | Databricks Workflows (managed, JSON-defined) |
| **Data Warehouse** | BigQuery | Databricks SQL Warehouse via Unity Catalog |
| **Transformation** | dbt with `dbt-bigquery` adapter | dbt with `dbt-databricks` adapter |
| **Governance** | BigQuery datasets + IAM | Unity Catalog (catalog → schema → table) |
| **Dashboard** | Google Data Studio | Databricks SQL Dashboards or any BI tool via SQL Warehouse |

---

## Stream Processing

### What changed

| Aspect | Original | Databricks |
|---|---|---|
| Spark session | Created manually via `SparkSession.builder` with `master("yarn")` | Provided by the Databricks runtime — no manual session creation needed |
| Kafka connector | Required `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2` | Pre-installed in the Databricks runtime |
| Write sink | `writeStream.format("parquet")` → Parquet files partitioned by `month/day/hour` on GCS | `writeStream.format("delta").toTable(...)` → Delta Lake managed table in Unity Catalog |
| Checkpoint | GCS path (`gs://bucket/checkpoint/topic`) | DBFS or Unity Catalog Volumes path |
| Trigger interval | `processingTime="120 seconds"` | Same `processingTime="120 seconds"` for continuous mode; `availableNow=True` for batch-triggered runs |
| String decode UDF | Kept as-is | Kept as-is — UDFs work identically on Databricks |

### Files

| Original | Databricks |
|---|---|
| `spark_streaming/stream_all_events.py` | `databricks/notebooks/stream_all_events.py` |
| `spark_streaming/streaming_functions.py` | `databricks/notebooks/streaming_functions.py` |
| `spark_streaming/schema.py` | Inlined in `stream_all_events.py` for notebook self-containment |
| *(new)* | `databricks/notebooks/ingest_events_autoloader.py` — Auto Loader alternative |
| *(new)* | `databricks/notebooks/load_songs.py` — replaces `load_songs_dag.py` |
| *(new)* | `databricks/notebooks/run_dbt.py` — dbt runner for Workflows |

### Key decisions

1. **Kept the Kafka source for real-time streaming.** The original pipeline reads from Kafka in real-time; this path is preserved in `stream_all_events.py` for environments where Kafka is accessible from Databricks.

2. **Added Auto Loader as a parallel ingestion path.** For migration scenarios where Kafka is not directly reachable (e.g. network isolation), or for backfilling historical Parquet files, `ingest_events_autoloader.py` uses `cloudFiles` to incrementally process files from cloud storage.

3. **Delta Lake replaces Parquet + external tables.** The original pipeline wrote Parquet files to GCS, then Airflow created temporary BigQuery external tables to load them. With Delta Lake, data is written directly to managed tables — no external table creation/deletion dance needed.

---

## Orchestration

### Airflow → Databricks Workflows mapping

The original `streamify_dag.py` ran hourly at minute 5 (`5 * * * *`) and executed this task chain for each of the 3 event types, then dbt:

```
create_external_table → create_empty_table → insert_job → delete_external_table → dbt_initiate → dbt_run
```

The Databricks Workflow (`streamify_hourly_pipeline`) simplifies this to:

```
load_songs (one-time) → ingest_all_events (sequential per topic) → dbt_seed → dbt_run
```

| Airflow Task | Databricks Task | Notes |
|---|---|---|
| `create_external_table` | Eliminated | Auto Loader handles incremental file discovery natively |
| `create_empty_table` | Eliminated | Delta tables are created automatically on first write |
| `insert_job` | `ingest_all_events` | Auto Loader replaces the INSERT SELECT from external table pattern; single task processes all topics sequentially to avoid checkpoint conflicts |
| `delete_external_table` | Eliminated | No temporary external tables to clean up |
| `dbt_initiate` | `dbt_seed` | Runs `dbt seed --select state_codes` |
| `dbt_streamify_run` | `dbt_run` | Runs `dbt run` |
| `load_songs_dag` (separate DAG) | `load_songs` | Consolidated into the main workflow as the first task |

### Workflow files

| File | Purpose |
|---|---|
| `databricks/workflows/streamify_workflow.json` | Hourly batch pipeline (replaces `streamify_dag.py` + `load_songs_dag.py`) |
| `databricks/workflows/stream_continuous_workflow.json` | Continuous streaming job (replaces `spark-submit` on YARN) |

### Schedule

- **Hourly pipeline:** Quartz cron `0 5 * * * ?` (every hour at minute 5), matching the original Airflow schedule.
- **Continuous streaming:** No schedule — runs continuously until manually stopped. Uses Databricks' `continuous` run mode.

---

## Transformation

### dbt adapter change

| Aspect | Original | Databricks |
|---|---|---|
| Adapter | `dbt-bigquery` | `dbt-databricks` |
| Connection | Service account keyfile + GCP project ID | Databricks host + HTTP path + personal access token |
| Catalog/Database | BigQuery project | Unity Catalog catalog name |
| Schema | BigQuery dataset (`streamify_stg`, `streamify_prod`) | Unity Catalog schema (`streamify_dev`, `streamify_prod`) |
| `dbt_utils` version | `0.8.0` | `>=1.0.0` (required for `dbt-databricks` compatibility) |

### SQL syntax changes

| BigQuery SQL | Databricks SQL | Files affected |
|---|---|---|
| `GENERATE_TIMESTAMP_ARRAY(start, end, INTERVAL 1 HOUR)` + `UNNEST(...)` | `explode(sequence(start, end, INTERVAL 1 HOUR))` | `dim_datetime.sql` |
| `date_trunc(ts, HOUR)` | `date_trunc('HOUR', ts)` | `fact_streams.sql` |
| `UNIX_SECONDS(date)` | `UNIX_TIMESTAMP(date)` | `dim_datetime.sql` |
| `EXTRACT(DAYOFWEEK FROM date) IN (6,7)` | `DAYOFWEEK(date) IN (1, 7)` | `dim_datetime.sql` — **Semantic fix:** the original BigQuery code flagged days 6 (Friday) and 7 (Saturday) as weekends, which was incorrect since BigQuery DAYOFWEEK returns 1=Sunday..7=Saturday. The Databricks version corrects this to 1 (Sunday) and 7 (Saturday). The `weekendFlag` column will differ during parallel-run validation. |
| `EXTRACT(DAYOFWEEK FROM date)` | `DAYOFWEEK(date)` | `dim_datetime.sql` |
| `EXTRACT(DAY FROM date)` | `DAY(date)` | `dim_datetime.sql` |
| `EXTRACT(WEEK FROM date)` | `WEEKOFYEAR(date)` | `dim_datetime.sql` |
| `EXTRACT(MONTH FROM date)` | `MONTH(date)` | `dim_datetime.sql` |
| `EXTRACT(YEAR FROM date)` | `YEAR(date)` | `dim_datetime.sql` |
| `DATE '9999-12-31'` | `CAST('9999-12-31' AS DATE)` | `dim_users.sql` |
| `{{ dbt_utils.surrogate_key([...]) }}` | `{{ dbt_utils.generate_surrogate_key([...]) }}` | All dim models |
| `partition_by` on views | Removed (Databricks views don't support partitioning) | `wide_streams.sql` |
| `partition_by` on `fact_streams` table | Removed (use Z-ORDER instead) | `fact_streams.sql` |

### Materializations

| Model | Original | Databricks | Notes |
|---|---|---|---|
| Staging models | `view` | `view` | No change |
| `dim_*` tables | `table` | `table` | Materialized as Delta tables |
| `fact_streams` | `table` with hourly partition | `table` | Partition removed; use Z-ORDER on `ts` post-creation |
| `wide_streams` | `view` with partition | `view` | Partition config removed (not supported on views) |

### Source configuration

The original `schema.yml` referenced BigQuery:
```yaml
database: "{{ env_var('GCP_PROJECT_ID') }}"
schema: streamify_stg
```

The Databricks version references Unity Catalog:
```yaml
database: streamify_catalog
schema: bronze
```

---

## Unity Catalog Schema Layout

```
streamify_catalog
├── bronze                          # Raw ingested data (replaces GCS Parquet + BigQuery staging)
│   ├── listen_events               # Delta table — from Kafka or Auto Loader
│   ├── page_view_events            # Delta table
│   ├── auth_events                 # Delta table
│   ├── songs                       # Delta table — loaded from CSV seed
│   └── _checkpoints/               # Structured Streaming checkpoints (Volume)
│       ├── kafka/
│       │   ├── listen_events/
│       │   ├── page_view_events/
│       │   └── auth_events/
│       └── autoloader/
│           ├── listen_events/
│           ├── page_view_events/
│           └── auth_events/
├── streamify_dev                   # dbt dev target (replaces streamify_stg BigQuery dataset)
│   ├── state_codes                 # dbt seed
│   ├── dim_users
│   ├── dim_songs
│   ├── dim_artists
│   ├── dim_location
│   ├── dim_datetime
│   ├── fact_streams
│   └── wide_streams                # view
└── streamify_prod                  # dbt prod target (replaces streamify_prod BigQuery dataset)
    ├── state_codes
    ├── dim_users
    ├── dim_songs
    ├── dim_artists
    ├── dim_location
    ├── dim_datetime
    ├── fact_streams
    └── wide_streams                # view
```

### Rationale

- **`bronze`** contains raw ingested data, analogous to the original GCS Parquet files + BigQuery staging tables.
- **`streamify_dev` / `streamify_prod`** contain the dbt-transformed star schema, analogous to the original BigQuery `streamify_stg` and `streamify_prod` datasets.
- Unity Catalog provides fine-grained access control at the catalog, schema, and table levels — replacing the combination of GCS IAM + BigQuery dataset permissions.

---

## Delta Lake Partitioning Strategy

### Why we removed explicit partitioning

The original pipeline used two partitioning strategies:
1. **PySpark write:** `.partitionBy("month", "day", "hour")` on Parquet files
2. **dbt/BigQuery:** `partition_by` with hourly timestamp granularity on `fact_streams`

For Delta Lake on Databricks, we chose **not** to use explicit partitioning for these reasons:

1. **Small data volumes.** The Streamify dataset generates modest data volumes. Over-partitioning creates many small files, which hurts read performance on Delta Lake.

2. **Delta Lake data skipping.** Delta Lake automatically collects min/max statistics on every column. Queries filtering on `ts` or `month/day/hour` columns already benefit from data skipping without explicit partitions.

3. **Z-ORDER is more flexible.** After initial table creation, running `OPTIMIZE ... ZORDER BY (ts)` provides better query performance than hourly partitioning for most analytical query patterns.

4. **Liquid clustering (recommended).** On Databricks Runtime 13.3+, liquid clustering (`CLUSTER BY (ts)`) is the preferred approach. It automatically manages data layout without manual OPTIMIZE commands.

### Recommended post-migration optimization

```sql
-- For fact_streams (run once after initial data load)
ALTER TABLE streamify_catalog.streamify_prod.fact_streams
  CLUSTER BY (ts);

-- For bronze tables (run periodically)
OPTIMIZE streamify_catalog.bronze.listen_events
  ZORDER BY (ts);
```

---

## Auto Loader vs Structured Streaming

| Criteria | Auto Loader (`cloudFiles`) | Structured Streaming (Kafka) |
|---|---|---|
| **Data source** | Files on cloud storage (GCS/S3/ADLS) | Kafka topics |
| **Use during migration** | Primary — processes existing Parquet files from GCS | After Kafka is accessible from Databricks |
| **Latency** | Minutes (triggered by Workflow schedule) | Seconds (continuous processing) |
| **Exactly-once** | Yes (checkpoint-based) | Yes (checkpoint-based) |
| **Schema evolution** | Built-in schema inference and evolution | Manual schema management |
| **Cost** | Lower — runs only when triggered | Higher — cluster runs continuously |
| **Recommended for** | Batch/micro-batch, backfill, migration bridge | Real-time requirements |

### Migration phasing

1. **Phase 1 (parallel run):** Keep the existing GCS Parquet pipeline running. Use Auto Loader to ingest the same files into Delta Lake. Validate data parity.
2. **Phase 2 (cutover):** Point the Kafka consumer directly at Databricks (using `stream_all_events.py`). Disable the legacy PySpark-on-YARN job.
3. **Phase 3 (decommission):** Remove Auto Loader workflow. Decommission GCS bucket, Airflow, and BigQuery resources.

---

## Workflow Scheduling

### Hourly batch pipeline (`streamify_hourly_pipeline`)

| Setting | Value | Rationale |
|---|---|---|
| Cron | `0 5 * * * ?` | Matches original Airflow `5 * * * *` |
| Max concurrent runs | 1 | Prevents overlapping runs (same as Airflow `max_active_runs=1`) |
| Cluster | Photon-enabled, 2 workers | Sufficient for the data volumes |
| Ingestion | Single task processes all 3 topics sequentially | Avoids checkpoint conflicts from parallel runs of the same notebook |
| Timeout | 3600s for ingestion task, 600s for dbt tasks | Generous limits for production reliability |

### Continuous streaming (`streamify_continuous_streaming`)

| Setting | Value | Rationale |
|---|---|---|
| Schedule | None (continuous) | Runs until stopped |
| Max concurrent runs | 1 | Single streaming job |
| Cluster | Photon-enabled, 2 workers | Matches original 2-worker YARN cluster |

---

## Migration Steps

### Prerequisites

- [ ] Databricks workspace provisioned with Unity Catalog enabled
- [ ] Network connectivity from Databricks to Kafka brokers (for streaming path)
- [ ] Cloud storage credentials configured in Unity Catalog external locations (for Auto Loader path)
- [ ] Databricks SQL Warehouse provisioned (for dbt)
- [ ] `dbt-databricks` adapter installed in the dbt execution environment
- [ ] Databricks personal access token or service principal for dbt connection

### Step-by-step

1. **Create Unity Catalog resources:**
   ```sql
   CREATE CATALOG IF NOT EXISTS streamify_catalog;
   CREATE SCHEMA IF NOT EXISTS streamify_catalog.bronze;
   CREATE SCHEMA IF NOT EXISTS streamify_catalog.streamify_dev;
   CREATE SCHEMA IF NOT EXISTS streamify_catalog.streamify_prod;
   ```

2. **Import notebooks** from `databricks/notebooks/` into the Databricks workspace (via Repos or manual import).

3. **Load songs reference data:** Run `load_songs.py` notebook once.

4. **Start ingestion:**
   - **Auto Loader path:** Deploy `streamify_workflow.json` workflow and trigger a manual run.
   - **Kafka path:** Deploy `stream_continuous_workflow.json` and start the continuous job.

5. **Validate bronze tables:** Verify row counts and data quality against the original BigQuery staging tables.

6. **Run dbt:**
   ```bash
   cd databricks/dbt
   dbt deps
   dbt seed --select state_codes --profiles-dir . --target prod
   dbt run --profiles-dir . --target prod
   ```

7. **Validate star schema:** Compare dimension and fact table row counts and key metrics against the original BigQuery production tables.

8. **Enable scheduled workflow:** Unpause the `streamify_hourly_pipeline` workflow.

9. **Decommission legacy infrastructure** after validation period.

---

## Rollback Plan

If the migration encounters critical issues:

1. The original GCP pipeline (Kafka → PySpark → GCS → Airflow → BigQuery → dbt) remains fully intact and unmodified.
2. All Databricks artifacts are in a separate `databricks/` directory — removing them has no impact on the original pipeline.
3. Databricks Workflows are created in `PAUSED` state — they do not run until explicitly enabled.
4. The original Airflow DAGs, PySpark scripts, and dbt project continue to function independently.
