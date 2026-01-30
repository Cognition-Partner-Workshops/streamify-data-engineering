# Streamify

A complete data engineering pipeline that simulates and processes a music streaming service's event data using Kafka, Spark Streaming, dbt, Docker, Airflow, Terraform, and GCP.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Pipeline Components](#pipeline-components)
- [Data Model](#data-model)
- [Dashboard](#dashboard)
- [Troubleshooting](#troubleshooting)
- [Future Improvements](#future-improvements)
- [Acknowledgments](#acknowledgments)
- [License](#license)

## Overview

Streamify is an end-to-end data pipeline that streams events from a simulated music streaming service (similar to Spotify) and processes them in real-time. The pipeline ingests user activity events such as song listens, page views, and authentication events, processes them through a streaming layer, and transforms them into analytics-ready tables for dashboarding.

The system generates approximately 1 million user events spread over 24 hours using [Eventsim](https://github.com/Interana/eventsim), which produces realistic fake event data based on the [Million Songs Dataset](http://millionsongdataset.com). Events are streamed through Kafka, processed by Spark Streaming every 2 minutes, and loaded into BigQuery hourly where dbt transforms them into a star schema for analytics. The Docker image for Eventsim is from [viirya's fork](https://github.com/viirya/eventsim), as the original project is no longer maintained.

![streamify-architecture](images/Streamify-Architecture.jpg)

## Architecture

The pipeline implements a Lambda Architecture with the following data flow:

**Speed Layer (Real-time):** Eventsim generates events and sends them to Kafka topics. Spark Streaming consumes these events and writes partitioned Parquet files to Google Cloud Storage every 2 minutes.

**Batch Layer (Hourly):** Airflow orchestrates hourly batch jobs that load the past hour's data from GCS into BigQuery staging tables, then triggers dbt to transform the data into production dimension and fact tables.

**Serving Layer:** BigQuery production dataset serves the transformed star schema to Google Data Studio for visualization and analytics.

## Technologies

| Component | Technology |
|-----------|------------|
| Cloud Platform | [Google Cloud Platform](https://cloud.google.com) |
| Infrastructure as Code | [Terraform](https://www.terraform.io) |
| Containerization | [Docker](https://www.docker.com), [Docker Compose](https://docs.docker.com/compose/) |
| Stream Processing | [Apache Kafka](https://kafka.apache.org), [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html) |
| Workflow Orchestration | [Apache Airflow](https://airflow.apache.org) |
| Data Transformation | [dbt](https://www.getdbt.com) |
| Data Lake | [Google Cloud Storage](https://cloud.google.com/storage) |
| Data Warehouse | [BigQuery](https://cloud.google.com/bigquery) |
| Visualization | [Google Data Studio](https://datastudio.google.com/overview) |
| Language | [Python](https://www.python.org) |

## Project Structure

```
streamify/
├── terraform/              # Infrastructure provisioning for GCP resources
│   ├── main.tf            # Resource definitions (VMs, Dataproc, BigQuery, GCS)
│   └── variables.tf       # Configurable parameters
├── kafka/                  # Kafka broker configuration
│   └── docker-compose.yml # Kafka, Zookeeper, Schema Registry, Control Center
├── spark_streaming/        # Spark streaming applications
│   ├── stream_all_events.py      # Main streaming orchestrator
│   ├── streaming_functions.py    # Reusable streaming utilities
│   └── schema.py                 # Event schema definitions
├── airflow/                # Batch orchestration
│   ├── dags/              # DAG definitions and SQL templates
│   ├── docker-compose.yaml # Airflow cluster (CeleryExecutor)
│   └── Dockerfile         # Custom Airflow image with dbt
├── dbt/                    # Data transformation models
│   ├── models/core/       # Dimension and fact table definitions
│   ├── seeds/             # Reference data (state codes)
│   └── profiles.yml       # BigQuery connection configuration
├── scripts/                # Setup and utility scripts
│   ├── vm_setup.sh        # VM initialization (Anaconda, Docker)
│   ├── airflow_startup.sh # Airflow deployment script
│   ├── eventsim_startup.sh # Event generation launcher
│   └── spark_setup.sh     # Spark installation script
├── setup/                  # Component-specific setup documentation
└── eventsim/              # Event simulation configuration
```

## Prerequisites

Before starting, ensure you have the following:

**Google Cloud Platform Account:** You will need a GCP account with billing enabled. New accounts receive $300 in free credits. Follow the [GCP Account Setup Guide](setup/gcp.md) to configure your project and service account. For Windows users, see the [alternate gcloud installation method](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/windows.md#google-cloud-sdk).

**Terraform:** Install Terraform (version 1.0 or higher) on your local machine. See the [Terraform Installation Guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/windows.md#terraform) for instructions.

**SSH Configuration:** Configure SSH access to your GCP VMs. The [SSH Setup Guide](setup/ssh.md) covers key generation and port forwarding.

## Quick Start

> **Warning:** You will be charged for all infrastructure provisioned. New GCP accounts can use the $300 free credit.

A complete video walkthrough is available on [YouTube](https://youtu.be/vzoYhI8KTlY).

### 1. Provision Infrastructure

Clone the repository and deploy GCP resources with Terraform. See the [detailed Terraform setup guide](setup/terraform.md) for more information.

```bash
git clone https://github.com/ankurchavda/streamify.git
cd streamify/terraform

terraform init
terraform plan    # Enter your GCS bucket name and GCP project ID when prompted
terraform apply
```

This creates the following resources:
- Kafka VM (e2-standard-4) for event streaming
- Airflow VM (e2-standard-4) for orchestration
- Dataproc Spark cluster (1 master + 2 workers) for stream processing
- GCS bucket for the data lake
- BigQuery datasets (streamify_stg, streamify_prod)
- Firewall rule for Kafka port 9092

### 2. Start Kafka and Event Generation

SSH into the Kafka VM and start the services. See the [detailed Kafka setup guide](setup/kafka.md) for more information.

```bash
ssh streamify-kafka

# Clone repo and install dependencies
git clone https://github.com/ankurchavda/streamify.git
bash ~/streamify/scripts/vm_setup.sh
exec newgrp docker

# Set environment variables
export KAFKA_ADDRESS=<KAFKA_VM_EXTERNAL_IP>

# Start Kafka
cd ~/streamify/kafka
docker-compose build && docker-compose up -d

# Start event generation (in a new terminal)
bash ~/streamify/scripts/eventsim_startup.sh
```

Verify Kafka is running by accessing the Control Center at `http://<KAFKA_VM_IP>:9021`. You should see four topics: `listen_events`, `page_view_events`, `auth_events`, and `status_change_events`.

![topics](images/topics.png)

### 3. Start Spark Streaming

SSH into the Spark master node and start consuming events. See the [detailed Spark setup guide](setup/spark.md) for more information.

```bash
ssh streamify-spark

git clone https://github.com/ankurchavda/streamify.git
cd streamify/spark_streaming

# Set environment variables
export KAFKA_ADDRESS=<KAFKA_VM_EXTERNAL_IP>
export GCP_GCS_BUCKET=<YOUR_BUCKET_NAME>

# Start streaming
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
  stream_all_events.py
```

Parquet files will appear in your GCS bucket every 2 minutes, partitioned by month/day/hour.

### 4. Start Airflow

SSH into the Airflow VM and deploy the orchestration layer. See the [detailed Airflow setup guide](setup/airflow.md) for more information.

```bash
ssh streamify-airflow

git clone https://github.com/ankurchavda/streamify.git
bash ~/streamify/scripts/vm_setup.sh
exec newgrp docker

# Copy your GCP service account credentials
# Place google_credentials.json in ~/.google/credentials/

# Set environment variables
export GCP_PROJECT_ID=<YOUR_PROJECT_ID>
export GCP_GCS_BUCKET=<YOUR_BUCKET_NAME>

# Start Airflow
bash ~/streamify/scripts/airflow_startup.sh
```

Access Airflow at `http://<AIRFLOW_VM_IP>:8080` (username: airflow, password: airflow).

### 5. Run the DAGs

In the Airflow UI:

1. First, trigger `load_songs_dag` once to load the reference song data into BigQuery
2. Then enable `streamify_dag` which runs hourly at the 5th minute to process events and run dbt transformations

![streamify_dag](images/streamify_dag.png)

### 6. Teardown

When finished, destroy all resources to stop billing:

```bash
cd streamify/terraform
terraform destroy
```

## Pipeline Components

### Kafka Event Streaming

Kafka receives events from Eventsim and buffers them in four topics:
- `listen_events` - Song play events with artist, song, and duration
- `page_view_events` - Website navigation events
- `auth_events` - Login/logout authentication events
- `status_change_events` - User status changes

The Kafka cluster runs via Docker Compose with Zookeeper, Schema Registry, and Confluent Control Center for monitoring.

![kafka](images/kafka.jpg)

### Spark Streaming

The Spark streaming application (`stream_all_events.py`) consumes from three Kafka topics simultaneously, applies schemas, transforms timestamps, and writes partitioned Parquet files to GCS every 2 minutes. The output is organized as:

```
gs://<bucket>/
├── listen_events/month=M/day=D/hour=H/*.parquet
├── page_view_events/month=M/day=D/hour=H/*.parquet
└── auth_events/month=M/day=D/hour=H/*.parquet
```

![spark](images/spark.jpg)

### Airflow Orchestration

The `streamify_dag` runs hourly at the 5th minute with the following workflow for each event type:

1. Create external table pointing to the past hour's GCS Parquet files
2. Create staging table (if not exists) with hourly partitioning
3. Insert data from external table to staging table
4. Delete external table
5. Run dbt transformations

![airflow](images/airflow.jpg)

### dbt Transformations

dbt transforms staging data into a star schema in the production dataset:

![dbt](images/dbt.png)

**Dimensions:**
- `dim_users` - User information with SCD Type 2 for subscription level tracking
- `dim_artists` - Artist metadata
- `dim_songs` - Song details
- `dim_location` - Geographic dimensions with state codes
- `dim_datetime` - Time dimensions (2018-2023, hourly granularity)

**Facts:**
- `fact_streams` - Central fact table partitioned by timestamp, linking all dimensions

**Views:**
- `wide_streams` - Denormalized view for simplified dashboard queries

## Data Model

The data model implements a star schema optimized for analytics queries:

```
                    ┌─────────────┐
                    │  dim_users  │
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴───────┐    ┌──────────────┐
│ dim_artists │────│ fact_streams  │────│  dim_songs   │
└─────────────┘    └───────┬───────┘    └──────────────┘
                           │
              ┌────────────┼────────────┐
              │                         │
       ┌──────┴──────┐          ┌───────┴───────┐
       │dim_location │          │ dim_datetime  │
       └─────────────┘          └───────────────┘
```

The `dim_users` table implements Slowly Changing Dimension Type 2 to track historical changes in user subscription levels (free/paid), enabling analysis of user behavior before and after subscription changes.

## Dashboard

The final dashboard in Google Data Studio provides insights into streaming activity:

![dashboard](images/dashboard.png)

Metrics include popular songs, active users, user demographics, listening patterns by time of day, and subscription level distribution.

## Troubleshooting

Common issues and solutions are documented in the [Debug Guide](setup/debug.md).

**Kafka containers failing to start:** If the broker or schema-registry containers die during startup, run `docker-compose down` and then `docker-compose up` again.

**Eventsim container name conflict:** If you see "container name /million_events is already in use", run `docker system prune` to clean up.

**Environment variables not persisting:** Environment variables must be set in each new shell session. Consider adding them to your `.bashrc` or `.profile`.

## Future Improvements

- Use managed services (Cloud Composer for Airflow, Confluent Cloud for Kafka)
- Create a dedicated VPC network for improved security
- Build dimensions and facts incrementally instead of full refresh
- Add data quality tests with dbt tests or Great Expectations
- Create dimensional models for additional business processes
- Implement CI/CD pipelines
- Add more dashboard visualizations

## Acknowledgments

This project was built as part of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by [DataTalks.Club](https://datatalks.club). The course provides excellent free resources for learning data engineering technologies.

The Eventsim Docker image is from [viirya's fork](https://github.com/viirya/eventsim) of the original [Interana Eventsim](https://github.com/Interana/eventsim) project.

## License

This project is open source and available under the MIT License.

---

_Originally written and maintained by contributors and [Devin](https://devin.ai), with updates from the core team._
