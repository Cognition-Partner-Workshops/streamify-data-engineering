"""Upload generated data files to S3 bronze layer."""

from __future__ import annotations

import os
from pathlib import Path

import boto3
import click
from rich.console import Console
from rich.progress import BarColumn, Progress, TextColumn

from data_generation.config import settings

console = Console()


def _get_s3_client():
    return boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def _ensure_bucket(s3_client, bucket_name: str) -> None:
    """Create the S3 bucket if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        console.print(f"  Bucket [cyan]{bucket_name}[/cyan] exists")
    except s3_client.exceptions.ClientError:
        console.print(f"  Creating bucket [cyan]{bucket_name}[/cyan]...")
        if settings.aws_region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": settings.aws_region},
            )


@click.command()
@click.option("--input-dir", default=None, help="Directory containing generated data")
@click.option("--bucket", default=None, help="S3 bucket name")
@click.option("--prefix", default="bronze", help="S3 key prefix (tier)")
@click.option("--create-bucket", "create", is_flag=True, help="Create bucket if missing")
def main(input_dir: str | None, bucket: str | None, prefix: str, create: bool) -> None:
    """Upload generated trading data to S3 bronze layer."""
    data_dir = Path(input_dir) if input_dir else settings.output_dir
    bucket_name = bucket or settings.s3_bucket_name

    if not data_dir.exists():
        console.print(f"[red]Data directory not found: {data_dir}[/red]")
        console.print("Run [bold]generate-data[/bold] first to create datasets.")
        raise SystemExit(1)

    console.print("\n[bold blue]Uploading to S3[/bold blue]")
    console.print(f"  Source: {data_dir.resolve()}")
    console.print(f"  Bucket: s3://{bucket_name}/{prefix}/\n")

    s3 = _get_s3_client()

    if create:
        _ensure_bucket(s3, bucket_name)

    # Discover files to upload
    files = sorted(data_dir.rglob("*"))
    files = [f for f in files if f.is_file()]

    if not files:
        console.print("[yellow]No files found to upload.[/yellow]")
        raise SystemExit(1)

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Uploading files...", total=len(files))

        for file_path in files:
            # Preserve directory structure: bronze/instruments/instruments.parquet
            relative = file_path.relative_to(data_dir)
            s3_key = f"{prefix}/{relative}"

            s3.upload_file(
                str(file_path),
                bucket_name,
                s3_key,
                ExtraArgs={"ContentType": _content_type(file_path.suffix)},
            )
            progress.update(task, advance=1, description=f"Uploaded {s3_key}")

    console.print(f"\n[bold green]Uploaded {len(files)} files to s3://{bucket_name}/{prefix}/[/bold green]\n")


def _content_type(suffix: str) -> str:
    return {
        ".parquet": "application/octet-stream",
        ".csv": "text/csv",
        ".json": "application/json",
    }.get(suffix, "application/octet-stream")


if __name__ == "__main__":
    main()
