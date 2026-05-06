"""Main entry point: generate all financial trading datasets and write to disk."""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from data_generation.config import settings
from data_generation.generators.instruments import InstrumentGenerator
from data_generation.generators.market_data import MarketDataGenerator
from data_generation.generators.orders import OrderGenerator
from data_generation.generators.positions import PositionGenerator
from data_generation.generators.trades import TradeGenerator

console = Console()


def _write_dataset(df, name: str, output_dir: Path, fmt: str) -> Path:
    """Write a DataFrame to the specified format."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        path = output_dir / f"{name}.parquet"
        df.to_parquet(path, index=False, engine="pyarrow")
    elif fmt == "csv":
        path = output_dir / f"{name}.csv"
        df.to_csv(path, index=False)
    elif fmt == "json":
        path = output_dir / f"{name}.json"
        df.to_json(path, orient="records", lines=True)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    return path


@click.command()
@click.option("--output-dir", default=None, help="Output directory (default: from config)")
@click.option("--format", "fmt", default=None, type=click.Choice(["parquet", "csv", "json"]))
@click.option("--num-instruments", default=None, type=int)
@click.option("--num-trades", default=None, type=int)
@click.option("--num-orders", default=None, type=int)
@click.option("--num-market-data", default=None, type=int)
@click.option("--seed", default=42, type=int, help="Random seed for reproducibility")
def main(
    output_dir: str | None,
    fmt: str | None,
    num_instruments: int | None,
    num_trades: int | None,
    num_orders: int | None,
    num_market_data: int | None,
    seed: int,
) -> None:
    """Generate synthetic financial securities trading data."""
    out_dir = Path(output_dir) if output_dir else settings.output_dir
    out_fmt = fmt or settings.output_format

    n_instruments = num_instruments or settings.num_instruments
    n_trades = num_trades or settings.num_trades
    n_orders = num_orders or settings.num_orders
    n_market_data = num_market_data or settings.num_market_data_records

    console.print("\n[bold blue]Securities Trading Data Generator[/bold blue]")
    console.print(f"  Output: {out_dir.resolve()} ({out_fmt})")
    console.print(f"  Seed: {seed}\n")

    results = {}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        # 1. Instruments (reference data - generated first as other generators depend on it)
        task = progress.add_task(f"Generating {n_instruments} instruments...", total=None)
        inst_gen = InstrumentGenerator(num_instruments=n_instruments, seed=seed)
        instruments_df = inst_gen.generate()
        path = _write_dataset(instruments_df, "instruments", out_dir / "instruments", out_fmt)
        results["instruments"] = (len(instruments_df), path)
        progress.update(task, completed=True, description=f"[green]Instruments: {len(instruments_df)} records")

        # 2. Market Data
        task = progress.add_task(f"Generating {n_market_data:,} market data records...", total=None)
        md_gen = MarketDataGenerator(instruments_df, num_records=n_market_data, seed=seed)
        market_data_df = md_gen.generate(
            start_date=settings.start_date, end_date=settings.end_date
        )
        path = _write_dataset(market_data_df, "market_data", out_dir / "market_data", out_fmt)
        results["market_data"] = (len(market_data_df), path)
        progress.update(task, completed=True, description=f"[green]Market Data: {len(market_data_df):,} records")

        # 3. Orders
        task = progress.add_task(f"Generating {n_orders:,} orders...", total=None)
        ord_gen = OrderGenerator(instruments_df, num_orders=n_orders, seed=seed)
        orders_df = ord_gen.generate(
            start_date=settings.start_date, end_date=settings.end_date
        )
        path = _write_dataset(orders_df, "orders", out_dir / "orders", out_fmt)
        results["orders"] = (len(orders_df), path)
        progress.update(task, completed=True, description=f"[green]Orders: {len(orders_df):,} records")

        # 4. Trades
        task = progress.add_task(f"Generating {n_trades:,} trades...", total=None)
        trade_gen = TradeGenerator(instruments_df, num_trades=n_trades, seed=seed)
        trades_df = trade_gen.generate(
            start_date=settings.start_date, end_date=settings.end_date
        )
        path = _write_dataset(trades_df, "trades", out_dir / "trades", out_fmt)
        results["trades"] = (len(trades_df), path)
        progress.update(task, completed=True, description=f"[green]Trades: {len(trades_df):,} records")

        # 5. Positions
        task = progress.add_task("Generating position snapshots...", total=None)
        pos_gen = PositionGenerator(instruments_df, num_portfolios=settings.num_portfolios, seed=seed)
        positions_df = pos_gen.generate(
            start_date=settings.start_date, end_date=settings.end_date
        )
        path = _write_dataset(positions_df, "positions", out_dir / "positions", out_fmt)
        results["positions"] = (len(positions_df), path)
        progress.update(task, completed=True, description=f"[green]Positions: {len(positions_df):,} records")

    # Summary table
    table = Table(title="Generation Summary", show_header=True, header_style="bold cyan")
    table.add_column("Dataset", style="bold")
    table.add_column("Records", justify="right")
    table.add_column("File", style="dim")

    for name, (count, path) in results.items():
        table.add_row(name, f"{count:,}", str(path))

    console.print(table)
    console.print(f"\n[bold green]All datasets generated successfully in {out_dir.resolve()}[/bold green]\n")


if __name__ == "__main__":
    main()
