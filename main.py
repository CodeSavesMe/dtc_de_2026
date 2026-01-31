# main.py

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import click
from dotenv import load_dotenv
from loguru import logger

# Import internal project components
from docker_ingestion_pipeline.config import build_paths, configure_logging, load_loader_settings, LoaderSettings
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

# --- 1. Environment Setup ---
# Load .env from the same folder as main.py for local development
ENV_PATH = Path(__file__).resolve().parent / ".env"

# Allow system environment variables (Docker/Kestra) to override .env
load_dotenv(dotenv_path=ENV_PATH, override=False)



# --- 2. Helper Functions ---

def env_default(name: str, default: str | None = None) -> str | None:
    """Retrieve env var, returning default if None or empty."""
    val = os.getenv(name)
    return val if val not in (None, "") else default


def env_int(name: str, default: int) -> int:
    """Safely parse integer from env var."""
    val = env_default(name)
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def normalize_month(month: str | int) -> str:
    """Ensure month is 2 digits (e.g., '1' -> '01')."""
    return str(month).strip().zfill(2)


def build_dtc_taxi_url(taxi: str, year: int, month: str, file_format: str) -> str:
    """
    Construct the GitHub download URL.
    The 'file_format' argument ensures we target the correct extension (.parquet or .csv.gz)
    expected on the GitHub release page.
    """
    taxi = taxi.lower().strip()
    month = normalize_month(month)
    format_str = file_format.lower().strip()

    if taxi not in {"green", "yellow", "fhv"}:
        raise click.UsageError(f"Unknown taxi type: {taxi}")

    if "parquet" in format_str:
        ext = "parquet"
    elif "csv" in format_str:
        ext = "csv.gz"
    else:
        raise click.UsageError(f"Unsupported format for URL builder: {format_str}")

    return (
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/"
        f"{taxi}_tripdata_{year}-{month}.{ext}"
    )


def monthly_table_name(taxi: str, year: int, month: str) -> str:
    """Generate standard table name: yellow_tripdata_2024_01"""
    taxi = taxi.lower().strip()
    month = normalize_month(month)
    return f"{taxi}_tripdata_{year}_{month}"


def infer_table_name_from_url(url: str) -> str:
    """Infer table name from URL filename, handling common extensions."""
    filename = os.path.basename(urlparse(url).path)
    # Remove extensions like .parquet, .csv.gz
    name = re.sub(r"\.(csv|tsv|parquet)(\.gz)?$", "", filename, flags=re.IGNORECASE)
    # Sanitize name for SQL compatibility
    return name.replace("-", "_")


# --- 3. Dependency Injection Builders ---

def build_pg_client() -> PostgresClient:
    """Initialize Postgres client using environment variables."""
    db_host = env_default("DB_HOST", "localhost")
    db_port = env_int("DB_PORT", 5432)
    db_user = env_default("DB_USER", "postgres")
    db_password = env_default("DB_PASSWORD", "postgres")
    db_name = env_default("DB_NAME", "ny_taxi")
    db_schema = env_default("DB_SCHEMA", "public")

    logger.info(f"Target DB: {db_host}:{db_port}/{db_name} (User: {db_user})")

    return PostgresClient.from_params(
        user=str(db_user),
        password=str(db_password),
        host=str(db_host),
        port=db_port,
        db=str(db_name),
        schema=str(db_schema),
    )


def build_pipeline(
        pg_client: PostgresClient,
        loader_settings: LoaderSettings
) -> IngestionPipeline:

    """
    Assemble the IngestionPipeline.
    This injects all necessary strategies (Locking, Loading, Validating, Swapping).
    """
    return IngestionPipeline(
        db=pg_client,
        lock=AdvisoryLock(pg_client),
        schema=PostgresSchemaManager(pg_client, sample_rows=2000),

        # Loader configuration
        csv_loader=CsvCopyLoader(
            pg_client,
            chunk_size=loader_settings.chunk_size,
            batch_size=loader_settings.batch_size,
        ),
        tsv_loader=TsvCopyLoader(
            pg_client,
            chunk_size=loader_settings.chunk_size,
            batch_size=loader_settings.batch_size,
        ),
        parquet_loader=ParquetStreamLoader(
            pg_client,
            chunk_size=loader_settings.chunk_size,
            batch_size=loader_settings.batch_size,
        ),
        validator=PostgresStagingValidator(pg_client),
        swapper=AtomicSwapper(pg_client),
        optimizer=PostLoadOptimizer(pg_client),
    )


# --- 4. Main CLI Commands ---

# Configure context to allow wider help text formatting
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'], max_content_width=120)


@click.group(context_settings=CONTEXT_SETTINGS)
def cli() -> None:
    """
    NY TAXI DATA INGESTION TOOL

    Orchestrates the ETL process for NYC TLC Taxi Data.
    Suitable for running in Docker, Kestra, or Airflow.

    \b
    KEY FEATURES:
    - Format Detection: Automatically handles Parquet or CSV.
    - Atomic Swaps: Replaces tables safely without downtime.
    - Validation: Checks schema and data integrity before finalizing.
    - Configurable: Controlled via CLI flags or Environment Variables.

    \b
    USAGE EXAMPLES:
    1. Standard Ingest:
       $ python main.py ingest --taxi yellow --year 2024 --month 01

    2. Using Environment Variables:
       $ export TAXI=yellow YEAR=2024 MONTH=01 IF_EXISTS=replace
       $ python main.py ingest
    """
    # Setup logging configuration on CLI start
    paths = build_paths()
    configure_logging(paths)


@cli.command("ingest", help="Standard Ingestion: Download and load monthly taxi data.")
@click.option("--taxi", type=click.Choice(["green", "yellow"], case_sensitive=False),
              default=lambda: env_default("TAXI"), required=True,
              help="Taxi type (GitHub folder name).")
@click.option("--year", type=int, default=lambda: env_int("YEAR", 2025), required=True,
              help="Trip year (YYYY).")
@click.option("--month", type=str, default=lambda: env_default("MONTH"), required=True,
              help="Trip month (MM).")
@click.option("--file-format", "file_format", type=click.Choice(["parquet", "csv"], case_sensitive=False),
              default=lambda: env_default("FILE_FORMAT", "parquet"), show_default=True,
              help="Source file format. Essential for constructing the correct download URL.")

@click.option("--if-exists", type=click.Choice(["skip", "replace", "fail", "append"]),
              default=lambda: env_default("IF_EXISTS", "replace"), show_default=True,
              help="Strategy if table exists. 'replace' performs an Atomic Swap.")
@click.option("--keep-local/--no-keep-local",
              default=lambda: (env_default("KEEP_LOCAL", "true") or "true").lower() in {"1", "true", "yes", "on"},
              show_default=True, help="Retain downloaded file on disk after processing.")

def ingest_cmd(
        taxi: str,
        year: int,
        month: str,
        file_format: str,
        if_exists: str,
        keep_local: bool,
               ) -> None:

    """
    Executes the standard ingestion workflow.

    1. Constructs the URL based on the provided year/month/format.
    2. Checks destination table status.
    3. Triggers the pipeline (Download -> Staging -> Swap).
    """
    taxi = taxi.lower()
    month = normalize_month(month)

    # 1. Construct URL
    try:
        url = build_dtc_taxi_url(taxi, year, month, file_format)
    except Exception as error:
        logger.error(f"Failed to build URL: {error}")
        sys.exit(1)

    table_name = monthly_table_name(taxi, year, month)

    logger.info("-" * 50)
    logger.info(f"JOB START: {taxi.upper()} {year}-{month}")
    logger.info(f"URL      : {url}")
    logger.info(f"Table    : {table_name}")
    logger.info(f"Strategy : {if_exists}")
    logger.info("-" * 50)

    try:
        pg_client = build_pg_client()

        # 2. Pre-flight check for existing table
        if pg_client.table_exists(table_name):
            if if_exists == "skip":
                logger.warning(f"Table {pg_client.schema}.{table_name} exists. Skipping.")
                return
            if if_exists == "fail":
                logger.error(f"Table {pg_client.schema}.{table_name} exists. Aborting.")
                sys.exit(2)
            if if_exists == "replace":
                logger.info("Table exists. Pipeline will use Atomic Swap (Staging -> Final).")
            if if_exists == "append":
                logger.info("Table exists. Pipeline will append rows (Staging -> INSERT -> Final).")

        # 3. Execute Pipeline
        loader_settings = load_loader_settings()
        pipeline = build_pipeline(pg_client, loader_settings)
        pipeline.run(
            url=url,
            table_name=table_name,
            keep_local=keep_local,
            if_exists=if_exists,
        )

        logger.success(f"Job Completed: {pg_client.schema}.{table_name}")

    except Exception as error:
        logger.exception(f"Ingestion Failed: {error}")
        sys.exit(1)


@cli.command("ingest-url", help="Custom Ingestion: Download from an arbitrary URL.")
@click.option("--url", default=lambda: env_default("DATA_URL"), required=True,
              help="Full URL to the raw file (Parquet/CSV).")

@click.option("--table-name", default=None,
              help="Target table name (Auto-inferred from filename if omitted).")

@click.option("--if-exists", type=click.Choice(["skip", "replace", "fail", "append"]),
              default=lambda: env_default("IF_EXISTS", "replace"), show_default=True,
              help="Strategy if table exists. 'replace' swaps staging->final; 'append' inserts staging into final.")

@click.option("--keep-local/--no-keep-local",
              default=lambda: (env_default("KEEP_LOCAL", "true") or "true").lower() in {"1", "true", "yes", "on"},
              show_default=True, help="Retain downloaded file on disk.")

def ingest_url_cmd(
        url: str,
        table_name: str | None,
        if_exists: str,
        keep_local: bool
) -> None:
    """
    Executes a custom ingestion job.
    Useful for ad-hoc files or sources outside the standard naming convention.
    """
    try:
        if not table_name:
            table_name = infer_table_name_from_url(url)

        logger.info(f"JOB START (Custom URL): {url}")
        logger.info(f"Target Table: {table_name}")
        logger.info(f"Strategy : {if_exists}")

        pg_client = build_pg_client()

        # Pre-flight check for existing table
        if pg_client.table_exists(table_name):
            if if_exists == "skip":
                logger.warning(f"Table {pg_client.schema}.{table_name} exists. Skipping.")
                return
            if if_exists == "fail":
                logger.error(f"Table {pg_client.schema}.{table_name} exists. Aborting.")
                sys.exit(2)
            if if_exists == "replace":
                logger.info("Table exists. Pipeline will use Atomic Swap (Staging -> Final).")
            if if_exists == "append":
                logger.info("Table exists. Pipeline will append rows (Staging -> INSERT -> Final).")

        loader_settings = load_loader_settings()
        pipeline = build_pipeline(pg_client, loader_settings)
        pipeline.run(
            url=url,
            table_name=table_name,
            keep_local=keep_local,
            if_exists=if_exists,
        )

        logger.success(f"Job Completed: {pg_client.schema}.{table_name}")

    except Exception as error:
        logger.exception(f"Custom Ingestion Failed: {error}")
        sys.exit(1)


if __name__ == "__main__":
    cli()