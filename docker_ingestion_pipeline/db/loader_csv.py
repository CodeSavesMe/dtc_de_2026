# docker_ingestion_pipeline/db/loader_csv.py

from __future__ import annotations

import gzip
from dataclasses import dataclass

from loguru import logger

from docker_ingestion_pipeline.ports.database import Database
from docker_ingestion_pipeline.ports.loader import Loader
from docker_ingestion_pipeline.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class CsvCopyLoader(Loader):
    """
    CSV loader using PostgreSQL COPY FROM STDIN.

    Notes:
    - Relies on the CSV header to map columns.
    - Supports both plain `.csv` and gzip-compressed `.csv.gz` files.
    - Uses a raw psycopg2 connection for maximum COPY performance.
    """
    db: Database

    def load(self, file_path: str, table_name: str) -> None:
        # Sanitize table name to avoid invalid identifiers or SQL injection
        table_name = sanitize_ident(table_name)

        # COPY statement expects CSV with header row
        copy_sql = f"COPY {qident(table_name)} FROM STDIN WITH (FORMAT csv, HEADER true)"

        logger.info(f"Starting COPY (CSV) into <green>{table_name}</green> ...")

        # Use raw connection to access psycopg2 COPY API
        raw_conn = self.db.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                # Automatically handle gzip-compressed CSVs
                opener = gzip.open if file_path.endswith(".gz") else open

                # Open file in text mode and stream directly into COPY
                with opener(file_path, "rt") as f:
                    cur.copy_expert(copy_sql, f)

            # Commit only after COPY succeeds
            raw_conn.commit()

        except Exception:
            # Roll back transaction on any failure
            raw_conn.rollback()
            raise

        finally:
            # Always close the raw connection
            raw_conn.close()

        logger.info(f"COPY finished for <green>{table_name}</green>")
