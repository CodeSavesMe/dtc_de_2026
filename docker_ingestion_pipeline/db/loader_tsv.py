# docker_ingestion_pipeline/db/loader_tsv.py

from __future__ import annotations

import gzip
from dataclasses import dataclass

from loguru import logger

from docker_ingestion_pipeline.ports.database import Database
from docker_ingestion_pipeline.ports.loader import Loader
from docker_ingestion_pipeline.utils.identifiers import sanitize_ident, qident

@dataclass
class TsvCopyLoader(Loader):
    db: Database

    def load(self, file_path: str, table_name: str) -> None:
        table_name = sanitize_ident(table_name)

        # PostgreSQL does not have a native TSV format.
        # Use CSV mode with tab character as the delimiter.
        copy_sql = (
            f"COPY {qident(table_name)} FROM STDIN "
            "WITH (FORMAT CSV, DELIMITER '\t', HEADER true, QUOTE '\"', ESCAPE '\"')"
        )

        logger.info(f"Starting COPY (TSV) into <green>{table_name}</green> ...")

        raw_conn = self.db.raw_connection()
        try:
            with raw_conn.cursor() as cur:

                # Choose the correct opener based on file compression
                # 'rt' mode ensures proper text reading
                opener = gzip.open if file_path.endswith(".gz") else open
                with opener(file_path, "rt", encoding="utf-8") as f:
                    cur.copy_expert(copy_sql, f)

            raw_conn.commit()
        except Exception as e:
            raw_conn.rollback()
            logger.error(f"Failed to copy TSV: {e}")
            raise
        finally:
            raw_conn.close()

        logger.info(f"COPY finished for <green>{table_name}</green>")
