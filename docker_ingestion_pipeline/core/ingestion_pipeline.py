# docker_ingestion_pipeline/core/ingestion_pipeline.py

from __future__ import annotations

import os
from dataclasses import dataclass
from time import time
from typing import TYPE_CHECKING

from loguru import logger
from sqlalchemy import text

from docker_ingestion_pipeline.utils.file_types import detect_file_format, FileFormat
from docker_ingestion_pipeline.utils.downloader import download_file
from docker_ingestion_pipeline.utils.identifiers import sanitize_ident, qident

if TYPE_CHECKING:
    # Imported only for type hints.
    # This avoids circular imports at runtime.
    from docker_ingestion_pipeline.ports.database import Database
    from docker_ingestion_pipeline.ports.lock import LockManager
    from docker_ingestion_pipeline.ports.schema import SchemaManager
    from docker_ingestion_pipeline.ports.loader import Loader
    from docker_ingestion_pipeline.ports.validator import Validator
    from docker_ingestion_pipeline.ports.swapper import Swapper
    from docker_ingestion_pipeline.db.optimize import PostLoadOptimizer


@dataclass(frozen=True)
class IngestionPipeline:
    # Database access (connections, metadata, transactions)
    db: Database

    # Lock to prevent concurrent ingestion on the same table
    lock: LockManager

    # Handles creation of final and staging tables
    schema: SchemaManager

    # Loaders for different file formats
    csv_loader: Loader
    parquet_loader: Loader
    tsv_loader: Loader

    # Validates data after loading into staging
    validator: Validator

    # Atomically replaces final table with staging table
    swapper: Swapper

    # Runs post-load maintenance (e.g. ANALYZE)
    optimizer: PostLoadOptimizer

    def _get_loader(self, fmt: FileFormat) -> Loader:
        """
        Return the loader matching the detected file format.
        """
        loaders_map = {
            FileFormat.PARQUET: self.parquet_loader,
            FileFormat.CSV: self.csv_loader,
            FileFormat.TSV: self.tsv_loader,
        }
        loader = loaders_map.get(fmt)
        if not loader:
            raise ValueError(f"No loader configured for format: {fmt}")
        return loader

    def run(self, url: str, table_name: str, keep_local: bool = True) -> None:
        # Normalize table name
        table_name = sanitize_ident(table_name)

        # Use a staging table for safe loading
        staging_table = sanitize_ident(f"{table_name}__staging")

        # Lock key scoped per destination table
        lock_key = f"ingest:{table_name}"

        # Download source file
        file_path = download_file(url)

        # Detect file format (csv / tsv / parquet)
        fmt = detect_file_format(file_path)

        # Measure execution time
        start = time()

        try:
            # Ensure only one ingestion runs per table
            with self.lock.acquire(lock_key):

                # 1. Ensure final table exists
                if not self.db.table_exists(table_name):
                    self.schema.ensure_final_schema(
                        file_path=file_path,
                        final_table=table_name,
                    )

                # 2. Recreate staging table
                self.schema.recreate_staging_like_final(
                    final_table=table_name,
                    staging_table=staging_table,
                )

                # 3. Load data into staging
                loader = self._get_loader(fmt)
                loader.load(
                    file_path=file_path,
                    table_name=staging_table,
                )

                # 4. Validate loaded data
                expected_month = self.validator.infer_expected_month_from_table(
                    table_name
                )

                self.validator.validate_staging(
                    staging_table,
                    expected_month=expected_month,
                )

                # 5. Promote staging to final table
                self.swapper.swap_tables_atomically(
                    final_table=table_name,
                    staging_table=staging_table,
                )

                # Run post-load optimization
                self.optimizer.analyze(table_name)

            logger.success(
                f"Ingestion complete in <green>{time() - start:.2f}</green>s"
            )

        except Exception as e:
            logger.exception(f"Ingestion failed: {e}")

            # Best-effort cleanup of staging table
            try:
                with self.db.begin() as conn:
                    conn.execute(
                        text(f"DROP TABLE IF EXISTS {qident(staging_table)}")
                    )
            except Exception:
                logger.warning("Failed to drop staging table during cleanup.")

            raise

        finally:
            # Remove local file if requested
            if (not keep_local) and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    logger.warning(
                        f"Failed to remove local file: {file_path}"
                    )