# Securities Trading Data Platform

End-to-end modern data platform for financial securities trading data, built on **AWS S3** and **Databricks** with a **medallion architecture** (bronze → silver → gold).

## Architecture

```
Data Generation → S3 (Bronze/Silver/Gold) → Databricks (Delta Lake + Unity Catalog) → Analytics
```

See [docs/architecture.md](docs/architecture.md) for the full architecture diagram and detailed design.

## What's Included

| Component | Description |
|-----------|-------------|
| **Data Generation** | Python scripts to generate realistic trading data (instruments, trades, orders, market data, positions) |
| **Terraform IaC** | Infrastructure provisioning for S3 buckets, IAM, KMS, Databricks workspace, Unity Catalog |
| **Databricks Notebooks** | 15 notebooks covering ingestion (bronze), transformation (silver), and aggregation (gold) |
| **Workflow Orchestration** | Databricks Workflow DAG with parallel task execution and dependency management |

## Data Domains

| Domain | Description | Volume |
|--------|-------------|--------|
| **Instruments** | Reference data: 500 securities across equities, ETFs, bonds, options, futures | ~500 records |
| **Trades** | Trade executions with pricing, commissions, settlement dates | ~100K records |
| **Orders** | Order book with limit/market/stop orders, fill rates, slippage | ~150K records |
| **Market Data** | 5-minute OHLCV bars with bid/ask spreads and VWAP | ~500K records |
| **Positions** | Monthly portfolio snapshots with P&L and risk metrics | ~18K records |

## Quick Start

### Prerequisites
- Python 3.10+
- AWS account with S3 access
- Databricks workspace with Unity Catalog

### 1. Install Dependencies

```bash
cd securities-trading-data-platform
pip install -e .
```

### 2. Generate Sample Data

```bash
# Generate all datasets (default: Parquet format)
python -m data_generation.generate_all

# Or with custom options
python -m data_generation.generate_all \
  --output-dir ./my_data \
  --format parquet \
  --num-instruments 200 \
  --num-trades 50000 \
  --num-orders 75000 \
  --num-market-data 250000 \
  --seed 42
```

Output:
```
┌──────────────────────────────────────────────┐
│              Generation Summary               │
├──────────────┬──────────┬────────────────────┤
│ Dataset      │ Records  │ File               │
├──────────────┼──────────┼────────────────────┤
│ instruments  │      500 │ .../instruments/.. │
│ market_data  │  500,000 │ .../market_data/.. │
│ orders       │  150,000 │ .../orders/...     │
│ trades       │  100,000 │ .../trades/...     │
│ positions    │   18,000 │ .../positions/...  │
└──────────────┴──────────┴────────────────────┘
```

### 3. Upload to S3

```bash
# Set AWS credentials
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"

# Upload generated data to S3 bronze layer
python -m data_generation.upload_to_s3 --create-bucket

# Or specify custom bucket
python -m data_generation.upload_to_s3 \
  --input-dir ./my_data \
  --bucket my-trading-data-lake \
  --prefix bronze \
  --create-bucket
```

### 4. Provision Infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values

terraform init
terraform plan
terraform apply
```

### 5. Run the Pipeline

Import the notebooks into your Databricks workspace and either:
- **Manual**: Run notebooks in order (setup → bronze → silver → gold)
- **Scheduled**: Deploy the workflow from `databricks/workflows/trading_data_pipeline.json`

## Project Structure

```
securities-trading-data-platform/
├── README.md
├── pyproject.toml
├── docs/
│   └── architecture.md          # Detailed architecture documentation
├── data_generation/
│   ├── config.py                # Settings (env vars / .env)
│   ├── generate_all.py          # CLI entry point
│   ├── upload_to_s3.py          # S3 upload utility
│   ├── generators/
│   │   ├── instruments.py       # 500 securities across 5 asset classes
│   │   ├── trades.py            # Trade executions with T+2 settlement
│   │   ├── orders.py            # Order book with fill rate simulation
│   │   ├── market_data.py       # GBM price simulation (OHLCV)
│   │   └── positions.py         # Monthly portfolio snapshots
│   └── models/
│       └── schemas.py           # Pydantic data models
├── terraform/
│   ├── providers.tf             # AWS + Databricks providers
│   ├── variables.tf             # Input variables
│   ├── s3.tf                    # S3 bucket, KMS, IAM, lifecycle
│   ├── databricks.tf            # Unity Catalog, clusters, SQL warehouse
│   ├── outputs.tf               # Terraform outputs
│   └── terraform.tfvars.example # Example variable values
└── databricks/
    ├── notebooks/
    │   ├── 00_setup/
    │   │   └── setup_catalog.py       # Initialize Unity Catalog
    │   ├── 01_bronze/
    │   │   ├── ingest_instruments.py  # Full refresh (reference data)
    │   │   ├── ingest_trades.py       # Auto Loader incremental
    │   │   ├── ingest_orders.py       # Auto Loader incremental
    │   │   ├── ingest_market_data.py  # Auto Loader + date partition
    │   │   └── ingest_positions.py    # Auto Loader incremental
    │   ├── 02_silver/
    │   │   ├── transform_instruments.py  # SCD Type 1, dedup, classify
    │   │   ├── transform_trades.py       # Enrich, cost analysis
    │   │   ├── transform_orders.py       # Fill rate, slippage calc
    │   │   ├── transform_market_data.py  # Technical indicators
    │   │   └── transform_positions.py    # P&L validation, risk class
    │   └── 03_gold/
    │       ├── daily_pnl.py              # Daily P&L aggregation
    │       ├── trading_volume_analytics.py  # Volume by symbol/venue/time
    │       ├── portfolio_summary.py      # AUM, diversification, returns
    │       ├── risk_metrics.py           # VaR, concentration, trader risk
    │       └── market_analytics.py       # Daily summary, sector perf
    └── workflows/
        └── trading_data_pipeline.json   # Databricks Workflow DAG

```

## Gold Layer Tables

| Table | Description | Key Metrics |
|-------|-------------|-------------|
| `daily_pnl` | Daily P&L by portfolio | Gross notional, net cash flow, trading costs |
| `unrealized_pnl` | Position-level unrealized P&L | Market value, unrealized P&L, return % |
| `trading_volume_by_symbol` | Volume analytics per instrument | Trade count, notional, price range |
| `trading_volume_by_venue` | Volume by execution venue | Venue market share, avg commission |
| `intraday_volume_profile` | Volume by time-of-day | Session distribution, notional by hour |
| `portfolio_summary` | Portfolio overview | AUM, return %, diversification score |
| `portfolio_sector_allocation` | Sector weights per portfolio | Sector value, weight %, sector P&L |
| `portfolio_var` | Value at Risk (95%) | VaR, VaR/AUM ratio, risk rating |
| `concentration_risk` | Portfolio concentration (HHI) | HHI, effective positions, risk class |
| `trader_risk_scores` | Trader activity profiling | Daily notional, activity score, risk tier |
| `daily_market_summary` | Daily OHLCV per symbol | Return %, range %, SMA trend signal |
| `sector_performance` | Sector-level returns | Avg return, breadth ratio, volatility |
| `liquidity_analysis` | Instrument liquidity scoring | Avg volume, spread, liquidity score |

## Configuration

All settings can be configured via environment variables (prefix: `TRADING_`) or a `.env` file:

| Variable | Default | Description |
|----------|---------|-------------|
| `TRADING_S3_BUCKET_NAME` | `securities-trading-data-platform` | S3 bucket name |
| `TRADING_AWS_REGION` | `us-east-1` | AWS region |
| `TRADING_NUM_INSTRUMENTS` | `500` | Number of instruments to generate |
| `TRADING_NUM_TRADES` | `100000` | Number of trade records |
| `TRADING_NUM_ORDERS` | `150000` | Number of order records |
| `TRADING_NUM_MARKET_DATA_RECORDS` | `500000` | Number of market data bars |
| `TRADING_START_DATE` | `2024-01-02` | Simulation start date |
| `TRADING_END_DATE` | `2024-12-31` | Simulation end date |
| `TRADING_OUTPUT_FORMAT` | `parquet` | Output format (parquet/csv/json) |

## Pipeline Schedule

The Databricks Workflow runs **Monday–Friday at 6:00 PM ET** (after US market close):

```
setup → bronze (5 parallel) → silver_instruments → silver (4 parallel) → gold (5 parallel)
```

Total estimated runtime: ~15-30 minutes depending on data volume.
