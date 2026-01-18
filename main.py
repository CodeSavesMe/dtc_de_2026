# 01-docker-terraform/main.py

import argparse
import os
import re
from pathlib import Path
from urllib.parse import urlparse

# External libraries
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


def infer_table_name_from_url(url: str) -> str:
    """
    Infer table name from the file name in the URL.
    Example:
    - yellow_tripdata_2021-01.csv.gz -> yellow_tripdata_2021_01
    """
    filename = os.path.basename(urlparse(url).path)
    name = re.sub(r"\.csv(\.gz)?$|\.parquet$", "", filename)
    return name.replace("-", "_")


def main():
    # Initialize logging and paths
    paths = build_paths()
    configure_logging(paths)

    parser = argparse.ArgumentParser(
        description="DTC ETL ingestion with .env support"
    )

    # Database connection settings
    parser.add_argument("--user", default=os.getenv("DB_USER", "postgres"), help="Postgres username")
    parser.add_argument("--password", default=os.getenv("DB_PASSWORD", "postgres"), help="Postgres password")
    parser.add_argument("--host", default=os.getenv("DB_HOST", "localhost"), help="Postgres host")
    parser.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "5432")), help="Postgres port")
    parser.add_argument("--db", default=os.getenv("DB_NAME", "ny_taxi"), help="Postgres database name")
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "public"), help="Postgres schema")

    # Data source
    parser.add_argument("--url", default=os.getenv("DATA_URL"), help="Source data URL")
    parser.add_argument(
        "--table_name",
        required=False,
        default=None,
        help="Target table name (derived from URL if omitted)",
    )

    # Runtime options
    parser.add_argument(
        "--keep_local",
        default=os.getenv("KEEP_LOCAL", "true").lower() == "true",
        help="Keep downloaded file after ingestion",
    )
    parser.add_argument(
        "--parquet_batch_size",
        type=int,
        default=int(os.getenv("PARQUET_BATCH_SIZE", "100000")),
        help="Number of rows per parquet batch",
    )

    args = parser.parse_args()

    if not args.url:
        logger.error("DATA_URL not provided (CLI or .env)")
        return

    if not args.table_name:
        args.table_name = infer_table_name_from_url(args.url)
        logger.info(f"Derived table name: <green>{args.table_name}</green>")

    logger.info(f"Starting ingestion for table <green>{args.table_name}</green>")
    logger.debug(f"Source URL: {args.url}")

    # Initialize Postgres client
    db = PostgresClient.from_params(
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        db=args.db,
        schema=args.schema,
    )

    # Build ingestion pipeline
    pipeline = IngestionPipeline(
        db=db,
        lock=AdvisoryLock(db),
        schema=PostgresSchemaManager(db, sample_rows=2000),
        csv_loader=CsvCopyLoader(db),
        tsv_loader=TsvCopyLoader(db),
        parquet_loader=ParquetStreamLoader(db, batch_size=args.parquet_batch_size),
        validator=PostgresStagingValidator(db),
        swapper=AtomicSwapper(db),
        optimizer=PostLoadOptimizer(db),
    )

    # Run ingestion
    pipeline.run(
        url=args.url,
        table_name=args.table_name,
        keep_local=args.keep_local,
    )

    logger.success(f"Successfully ingested <green>{args.table_name}</green>")


if __name__ == "__main__":
    main()
