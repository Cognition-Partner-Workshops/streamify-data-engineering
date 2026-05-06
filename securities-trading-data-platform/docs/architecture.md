# Architecture: Securities Trading Data Platform

## Overview

This platform implements a **medallion architecture** (bronze → silver → gold) for financial securities trading data, built on **AWS S3** and **Databricks**. It provides a complete data lakehouse for ingesting, transforming, and analyzing trading activity across equities, ETFs, fixed income, options, and futures.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                     │
│  ┌─────────────┐ ┌─────────────┐ ┌──────────────┐ ┌──────────────────┐ │
│  │ Instruments  │ │   Trades    │ │   Orders     │ │   Market Data    │ │
│  │ (Reference)  │ │ (Execution) │ │  (Book)      │ │   (OHLCV)        │ │
│  └──────┬──────┘ └──────┬──────┘ └──────┬───────┘ └───────┬──────────┘ │
│         │               │               │                 │            │
│  ┌──────┴───────────────┴───────────────┴─────────────────┴──────────┐ │
│  │              Data Generation Scripts (Python)                      │ │
│  │    Faker + NumPy + GBM simulation → Parquet files                  │ │
│  └──────────────────────────┬────────────────────────────────────────┘ │
└─────────────────────────────┼───────────────────────────────────────────┘
                              │ upload_to_s3.py
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          AWS S3 DATA LAKE                               │
│                                                                         │
│  ┌───────────────────┐  ┌───────────────────┐  ┌────────────────────┐  │
│  │     BRONZE         │  │      SILVER        │  │       GOLD         │  │
│  │  (Raw / Immutable) │  │  (Cleansed)        │  │  (Analytics)       │  │
│  │                    │  │                    │  │                    │  │
│  │  • instruments/    │  │  • instruments/    │  │  • daily_pnl/      │  │
│  │  • trades/         │→ │  • trades/         │→ │  • trading_volume/ │  │
│  │  • orders/         │  │  • orders/         │  │  • portfolio/      │  │
│  │  • market_data/    │  │  • market_data/    │  │  • risk_metrics/   │  │
│  │  • positions/      │  │  • positions/      │  │  • market_analytics│  │
│  └───────────────────┘  └───────────────────┘  └────────────────────┘  │
│                                                                         │
│  Lifecycle: Bronze→Glacier(1yr)  Silver→IT(180d)  Gold→Standard(hot)   │
│  Encryption: AWS KMS (SSE-KMS)   Versioning: Enabled                   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              │ External Location (Unity Catalog)
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       DATABRICKS PLATFORM                               │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    Unity Catalog                                  │   │
│  │  Catalog: trading_platform                                        │   │
│  │  Schemas: bronze │ silver │ gold                                  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │              Databricks Workflows (DAG)                           │   │
│  │                                                                    │   │
│  │   setup ──→ bronze_* ──→ silver_instruments ──→ silver_* ──→ gold_*│  │
│  │          (parallel)           ↓                (parallel)          │   │
│  │                          silver_trades ──→ gold_daily_pnl          │   │
│  │                          silver_orders      gold_trading_volume    │   │
│  │                          silver_market       gold_portfolio        │   │
│  │                          silver_positions    gold_risk_metrics     │   │
│  │                                              gold_market_analytics │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐   │
│  │ All-Purpose      │  │ SQL Warehouse    │  │ Delta Lake           │   │
│  │ Cluster (Dev)    │  │ (Analytics)      │  │ (ACID Tables)        │   │
│  │ i3.xlarge 1-4    │  │ Serverless/Small │  │ Auto-Optimize        │   │
│  └─────────────────┘  └──────────────────┘  │ Auto-Compact         │   │
│                                              └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      CONSUMPTION LAYER                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────┐  │
│  │ Databricks   │  │ BI Tools     │  │ Notebooks    │  │ REST API  │  │
│  │ SQL Editor   │  │ (Tableau,    │  │ (Ad-hoc      │  │ (Apps)    │  │
│  │              │  │  Power BI)   │  │  Analysis)   │  │           │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  └───────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Domains

### 1. Instruments (Reference Data)
Static reference data for tradeable securities. Covers 5 asset classes: equities, ETFs, fixed income, options, and futures across 11 market sectors.

### 2. Trades (Execution Data)
Individual trade executions with full lifecycle tracking. Includes pricing, commission/fee details, execution venue, T+2 settlement dates, and links to originating orders.

### 3. Orders (Order Book)
Order flow data covering market, limit, stop, and stop-limit order types. Tracks fill rates, slippage, and order lifecycle states.

### 4. Market Data (OHLCV)
5-minute intraday price bars with bid/ask spreads, VWAP, and volume data. Generated using Geometric Brownian Motion for realistic price simulation.

### 5. Positions (Portfolio Snapshots)
End-of-month position snapshots for 50 portfolios. Includes cost basis, market value, unrealized/realized P&L, and portfolio weight.

## Medallion Architecture

### Bronze Layer
- **Purpose**: Raw data landing zone — immutable, append-only
- **Ingestion**: Databricks Auto Loader (cloudFiles) for incremental file detection
- **Format**: Delta Lake tables with ingestion metadata (`_ingested_at`, `_source_file`)
- **Retention**: S3 lifecycle transitions to Intelligent-Tiering at 90 days, Glacier at 1 year

### Silver Layer
- **Purpose**: Cleansed, validated, and enriched data
- **Processing**: Batch merge/upsert with data quality filters
- **Enrichment**: Instrument metadata joins, derived calculations (fill rates, slippage, technical indicators)
- **Key transforms**:
  - Price integrity validation (high >= low, prices > 0)
  - Deduplication on business keys
  - Rolling technical indicators (SMA-5, SMA-20, 20-period volatility)
  - P&L recalculation and risk classification

### Gold Layer
- **Purpose**: Business-level aggregations for analytics and dashboards
- **Tables**:
  - `daily_pnl` — Portfolio P&L by day
  - `trading_volume_by_symbol` — Volume analytics per instrument
  - `portfolio_summary` — AUM, diversification, returns
  - `portfolio_var` — Value at Risk (95% parametric)
  - `concentration_risk` — HHI-based portfolio concentration
  - `trader_risk_scores` — Trader activity risk profiling
  - `daily_market_summary` — Daily OHLCV with trend signals
  - `sector_performance` — Sector return and breadth analytics
  - `liquidity_analysis` — Spread and volume liquidity scoring

## Infrastructure

### Terraform Resources
| Resource | Purpose |
|----------|---------|
| S3 Bucket | Data lake with bronze/silver/gold prefixes |
| KMS Key | Server-side encryption for data at rest |
| IAM Role/Policy | Databricks ↔ S3 access credentials |
| Unity Catalog | Governance and metadata management |
| All-Purpose Cluster | ETL development (i3.xlarge, 1-4 workers) |
| SQL Warehouse | Gold layer analytics queries |
| External Location | S3 access through Unity Catalog |

### Security
- KMS encryption (SSE-KMS) on all S3 data
- Public access blocked on all buckets
- IAM role-based access for Databricks (least-privilege)
- Unity Catalog for fine-grained data governance
- Single-user data security mode on clusters

## Pipeline Schedule
The Databricks Workflow runs **Mon-Fri at 6:00 PM ET** (after US market close):

1. **Setup** → Verify catalog and schemas
2. **Bronze** (parallel) → Ingest all 5 data domains from S3
3. **Silver Instruments** → Must complete first (reference data for joins)
4. **Silver** (parallel) → Transform remaining 4 domains
5. **Gold** (parallel) → Compute all analytics aggregations
