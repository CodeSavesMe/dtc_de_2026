# 01-docker-terraform/main.py

import os
import re
from pathlib import Path
from urllib.parse import urlparse

import click
from dotenv import load_dotenv
from loguru import logger

# Project modules
from docker_ingestion_pipeline.config import build_paths, configure_logging
from docker_ingestion_pipeline.core.ingestion_pipeline import IngestionPipeline
from docker_ingestion_pipeline.db.client import PostgresClient
from docker_ingestion_pipeline.db.loader_csv import CsvCopyLoader
from docker_ingestion_pipeline.db.loader_tsv import TsvCopyLoader
from docker_ingestion_pipeline.db.loader_parquet import ParquetStreamLoader
from docker_ingestion_pipeline.db.lock import AdvisoryLock
from docker_ingestion_pipeline.db.optimize import PostLoadOptimizer
from docker_ingestion_pipeline.db.schema import PostgresSchemaManager
from docker_ingestion_pipeline.db.swapper import AtomicSwapper
from docker_ingestion_pipeline.db.validator_repo import PostgresStagingValidator


# Load environment variables from .env
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)


def parse_env_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    if v in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def infer_table_name_from_url(url: str) -> str:
    """
    Infer table name from the file name in the URL.
    Example:
    - yellow_tripdata_2021-01.csv.gz -> yellow_tripdata_2021_01
    """
    filename = os.path.basename(urlparse(url).path)
    name = re.sub(r"\.(csv|tsv)(\.gz)?$|\.parquet$", "", filename)
    return name.replace("-", "_")


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--user", default=os.getenv("DB_USER", "postgres"), show_default=True, help="Postgres username")
@click.option("--password", default=os.getenv("DB_PASSWORD", "postgres"), show_default=True, help="Postgres password")
@click.option("--host", default=os.getenv("DB_HOST", "localhost"), show_default=True, help="Postgres host")
@click.option("--port", type=int, default=int(os.getenv("DB_PORT", "5432")), show_default=True, help="Postgres port")
@click.option("--db", "db_name", default=os.getenv("DB_NAME", "ny_taxi"), show_default=True, help="Postgres database name")
@click.option("--schema", default=os.getenv("DB_SCHEMA", "public"), show_default=True, help="Postgres schema")
@click.pass_context
def cli(ctx: click.Context, user: str, password: str, host: str, port: int, db_name: str, schema: str):
    """DTC ETL ingestion CLI with .env support."""
    # Initialize logging and paths once
    paths = build_paths()
    configure_logging(paths)

    # Store DB config in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj.update(
        {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "db": db_name,
            "schema": schema,
        }
    )


@cli.command()
@click.option("--url", default=os.getenv("DATA_URL"), help="Source data URL (or set DATA_URL in .env)")
@click.option(
    "--table-name",
    "--table_name",
    default=None,
    help="Target table name (derived from URL if omitted)",
)
@click.option(
    "--keep-local/--no-keep-local",
    "--keep_local/--no_keep_local",
    default=parse_env_bool(os.getenv("KEEP_LOCAL", "true"), default=True),
    show_default=True,
    help="Keep downloaded file after ingestion",
)
@click.option(
    "--parquet-batch-size",
    "--parquet_batch_size",
    type=int,
    default=int(os.getenv("PARQUET_BATCH_SIZE", "100000")),
    show_default=True,
    help="Number of rows per parquet batch",
)

@click.pass_context
def ingest(
    ctx: click.Context,
    url: str | None,
    table_name: str | None,
    keep_local: bool,
    parquet_batch_size: int,
):
    """Ingest data from a URL into Postgres."""
    if not url:
        logger.error("DATA_URL not provided (CLI or .env)")
        raise click.Abort()

    if not table_name:
        table_name = infer_table_name_from_url(url)
        logger.info(f"Derived table name: <green>{table_name}</green>")

    logger.info(f"Starting ingestion for table <green>{table_name}</green>")
    logger.debug(f"Source URL: {url}")

    # Initialize Postgres client from global options
    cfg = ctx.obj or {}
    db_client = PostgresClient.from_params(
        user=cfg.get("user"),
        password=cfg.get("password"),
        host=cfg.get("host"),
        port=cfg.get("port"),
        db=cfg.get("db"),
        schema=cfg.get("schema"),
    )

    # Build ingestion pipeline
    pipeline = IngestionPipeline(
        db=db_client,
        lock=AdvisoryLock(db_client),
        schema=PostgresSchemaManager(db_client, sample_rows=2000),
        csv_loader=CsvCopyLoader(db_client),
        tsv_loader=TsvCopyLoader(db_client),
        parquet_loader=ParquetStreamLoader(db_client, batch_size=parquet_batch_size),
        validator=PostgresStagingValidator(db_client),
        swapper=AtomicSwapper(db_client),
        optimizer=PostLoadOptimizer(db_client),
    )

    # Run ingestion
    pipeline.run(
        url=url,
        table_name=table_name,
        keep_local=keep_local,
    )

    logger.success(f"Successfully ingested <green>{table_name}</green>")


if __name__ == "__main__":
    cli()
