# Orchestration Migration: Airflow → Databricks Workflows

This document describes the migration of Apache Airflow DAGs to Databricks Workflow JSON definitions for the Streamify data engineering pipeline.

## Overview

| Airflow DAG | Databricks Workflow | Schedule | Tasks |
|---|---|---|---|
| `streamify_dag` | `databricks/workflows/streamify_dag.json` | Hourly (minute 5) | 14 |
| `load_songs_dag` | `databricks/workflows/load_songs_dag.json` | One-time (manual) | 2 |
| `dbt_test` | `databricks/workflows/dbt_test_dag.json` | One-time (manual) | 1 |

---

## DAG-to-Workflow Mapping

### 1. `streamify_dag` → `streamify_dag.json`

**Purpose**: Hourly ETL pipeline that ingests streaming event data from cloud storage into staging tables, then runs dbt transformations to produce dimensional models.

**Schedule Migration**:
- Airflow: `5 * * * *` (cron)
- Databricks: `0 5 * * * ?` (quartz cron)

**Task Mapping** (per event type × 3 events: `listen_events`, `page_view_events`, `auth_events`):

| Airflow Task | Airflow Operator | Databricks Task Key | Databricks Task Type |
|---|---|---|---|
| `{event}_create_external_table` | `BigQueryCreateExternalTableOperator` | `{event}_create_external_table` | `sql_task` — `CREATE TABLE ... USING PARQUET LOCATION` |
| `{event}_create_empty_table` | `BigQueryCreateEmptyTableOperator` | `{event}_create_empty_table` | `sql_task` — `CREATE TABLE IF NOT EXISTS ... USING DELTA` |
| `{event}_execute_insert_query` | `BigQueryInsertJobOperator` | `{event}_execute_insert_query` | `sql_task` — `INSERT INTO ... SELECT ... COALESCE(...)` |
| `{event}_delete_external_table` | `BigQueryDeleteTableOperator` | `{event}_delete_external_table` | `sql_task` — `DROP TABLE IF EXISTS` |
| `dbt_initiate` | `BashOperator` | `dbt_initiate` | `dbt_task` — `dbt deps`, `dbt seed --select state_codes` |
| `dbt_streamify_run` | `BashOperator` | `dbt_streamify_run` | `dbt_task` — `dbt deps`, `dbt run` |

**Dependency Chain** (preserved from Airflow):
```
listen_events_create_external_table → listen_events_create_empty_table → listen_events_execute_insert_query → listen_events_delete_external_table ─┐
page_view_events_create_external_table → ... → page_view_events_delete_external_table ──────────────────────────────────────────────────────────────┼→ dbt_initiate → dbt_streamify_run
auth_events_create_external_table → ... → auth_events_delete_external_table ────────────────────────────────────────────────────────────────────────┘
```

**Parameters**:
| Parameter | Default | Description |
|---|---|---|
| `catalog` | `streamify` | Unity Catalog catalog name |
| `schema` | `streamify_stg` | Target schema |
| `volume_path` | `/Volumes/streamify/streamify_stg/raw_data` | Unity Catalog volume path for raw parquet data |
| `execution_datetime_str` | _(runtime)_ | Timestamp suffix for temp tables (replaces Airflow `{{ logical_date }}`) |
| `execution_month` | _(runtime)_ | Execution month |
| `execution_day` | _(runtime)_ | Execution day |
| `execution_hour` | _(runtime)_ | Execution hour |
| `warehouse_id` | _(required)_ | Databricks SQL warehouse ID |
| `dbt_project_path` | `/Workspace/streamify/dbt` | Path to dbt project in Databricks workspace |

---

### 2. `load_songs_dag` → `load_songs_dag.json`

**Purpose**: One-time reference data load — downloads songs CSV, converts to Parquet, and creates a managed table.

**Schedule Migration**:
- Airflow: `@once`
- Databricks: No recurring schedule (manual trigger)

**Task Mapping**:

| Airflow Task | Airflow Operator | Databricks Task Key | Databricks Task Type |
|---|---|---|---|
| `download_songs_file` | `BashOperator` (curl) | `download_convert_upload_songs` | `notebook_task` (combined) |
| `convert_to_parquet` | `PythonOperator` (pyarrow) | _(combined above)_ | _(combined above)_ |
| `upload_to_gcs` | `PythonOperator` (google.cloud.storage) | _(combined above)_ | _(combined above)_ |
| `remove_files_from_local` | `BashOperator` (rm) | _(combined above)_ | _(combined above)_ |
| `create_external_table` | `BigQueryCreateExternalTableOperator` | `create_songs_table` | `sql_task` — creates managed table from parquet |

**Consolidation Note**: The first 4 Airflow tasks (download → convert → upload → cleanup) are consolidated into a single Databricks notebook task (`download_convert_upload_songs`), since Spark can natively read CSV and write to Unity Catalog volumes without intermediate steps.

**Parameters**:
| Parameter | Default | Description |
|---|---|---|
| `catalog` | `streamify` | Unity Catalog catalog name |
| `schema` | `streamify_stg` | Target schema |
| `volume_path` | `/Volumes/streamify/streamify_stg/songs_raw` | Volume path for songs data |
| `warehouse_id` | _(required)_ | Databricks SQL warehouse ID |

---

### 3. `dbt_test` → `dbt_test_dag.json`

**Purpose**: Test dbt setup by running dependency installation and compilation.

**Schedule Migration**:
- Airflow: `@once`
- Databricks: No recurring schedule (manual trigger)

**Task Mapping**:

| Airflow Task | Airflow Operator | Databricks Task Key | Databricks Task Type |
|---|---|---|---|
| `dbt_test` | `BashOperator` (`dbt deps && dbt compile`) | `dbt_test` | `dbt_task` — `dbt deps`, `dbt compile` |

**Parameters**:
| Parameter | Default | Description |
|---|---|---|
| `warehouse_id` | _(required)_ | Databricks SQL warehouse ID |
| `catalog` | _(required)_ | Unity Catalog catalog name |
| `schema` | _(required)_ | Target schema |

---

## Operator Migration Reference

### BigQuery → Databricks SQL

| BigQuery Operator | Databricks Equivalent | Notes |
|---|---|---|
| `BigQueryCreateExternalTableOperator` | `sql_task` with `CREATE TABLE ... USING PARQUET LOCATION` | GCS URIs → Unity Catalog Volume paths |
| `BigQueryCreateEmptyTableOperator` | `sql_task` with `CREATE TABLE IF NOT EXISTS ... USING DELTA` | BigQuery schema → Delta table DDL |
| `BigQueryInsertJobOperator` | `sql_task` with inline SQL query | COALESCE transforms preserved as-is |
| `BigQueryDeleteTableOperator` | `sql_task` with `DROP TABLE IF EXISTS` | Direct translation |
| `BigQueryCreateExternalTableOperator` (PARQUET) | `sql_task` with `CREATE TABLE ... USING PARQUET` | External → managed table pattern |

### General Operator Mapping

| Airflow Operator | Databricks Task Type | Notes |
|---|---|---|
| `BashOperator` (dbt commands) | `dbt_task` | Native Databricks dbt integration |
| `PythonOperator` | `notebook_task` | Python logic in Databricks notebooks |
| `BashOperator` (shell commands) | `notebook_task` | Wrapped in notebook with `%sh` magic or Python equivalent |

### Data Type Mapping (BigQuery → Databricks)

| BigQuery Type | Databricks Type |
|---|---|
| `STRING` | `STRING` |
| `FLOAT64` | `DOUBLE` |
| `INTEGER` | `BIGINT` |
| `TIMESTAMP` | `TIMESTAMP` |
| `BOOLEAN` | `BOOLEAN` |
| `NUMERIC` | `DOUBLE` (cast preserved) |

### Template Variable Migration

| Airflow (Jinja) | Databricks (Job Parameters) |
|---|---|
| `{{ logical_date.strftime("%-m") }}` | `{{job.parameters.execution_month}}` |
| `{{ logical_date.strftime("%-d") }}` | `{{job.parameters.execution_day}}` |
| `{{ logical_date.strftime("%-H") }}` | `{{job.parameters.execution_hour}}` |
| `{{ logical_date.strftime("%m%d%H") }}` | `{{job.parameters.execution_datetime_str}}` |
| `{{ BIGQUERY_DATASET }}` | `` `{{job.parameters.catalog}}`.`{{job.parameters.schema}}` `` |

### Storage Path Migration

| GCP (Airflow) | Databricks |
|---|---|
| `gs://{GCP_GCS_BUCKET}/{path}/*` | `/Volumes/{catalog}/{schema}/{volume}/{path}` |
| BigQuery dataset.table | `` `{catalog}`.`{schema}`.`{table}` `` (Unity Catalog) |

---

## Prerequisites for Deployment

1. **Databricks SQL Warehouse**: A running SQL warehouse with the `warehouse_id` configured
2. **Unity Catalog**: Catalog and schema created (default: `streamify.streamify_stg`)
3. **Unity Catalog Volumes**: Volume paths created for raw data storage
4. **dbt Project**: dbt project deployed to `/Workspace/streamify/dbt`
5. **Notebook**: `load_songs` notebook deployed to `/Workspace/Shared/streamify/load_songs`
6. **SQL File**: `create_songs_table.sql` deployed to `/Workspace/Shared/streamify/sql/`

## Deployment

Deploy workflows using the Databricks CLI or REST API:

```bash
# Deploy all workflows
for f in databricks/workflows/*.json; do
  databricks jobs create --json @"$f"
done

# Or via REST API
curl -X POST https://<workspace>.cloud.databricks.com/api/2.1/jobs/create \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -d @databricks/workflows/streamify_dag.json
```
