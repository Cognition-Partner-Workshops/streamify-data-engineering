"""Configuration for data generation and S3 upload."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    # S3 Configuration
    s3_bucket_name: str = "securities-trading-data-platform"
    aws_region: str = "us-east-1"

    # Data Generation
    num_instruments: int = 500
    num_trades: int = 100_000
    num_orders: int = 150_000
    num_market_data_records: int = 500_000
    num_trading_days: int = 252  # one trading year
    num_portfolios: int = 50

    # Date range
    start_date: str = "2024-01-02"
    end_date: str = "2024-12-31"

    # Output
    output_dir: Path = Path("generated_data")
    output_format: str = "parquet"  # parquet | csv | json

    # S3 paths (medallion architecture)
    bronze_prefix: str = "bronze"
    silver_prefix: str = "silver"
    gold_prefix: str = "gold"

    model_config = {"env_prefix": "TRADING_", "env_file": ".env"}


settings = Settings()
