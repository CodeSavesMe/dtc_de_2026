# docker_ingestion_pipeline/db/optimize.py
from __future__ import annotations

from dataclasses import dataclass
from loguru import logger
from sqlalchemy import text

from docker_ingestion_pipeline.ports.database import Database
from docker_ingestion_pipeline.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class PostLoadOptimizer:
    """
    Performs post-load database optimizations.

    Notes:
    - Currently supports running `ANALYZE` on a table to update PostgreSQL statistics.
    - Uses fully qualified table names with sanitized identifiers to prevent SQL issues.
    """
    db: Database

    def _schema_name(self) -> str:
        # Return the schema name of the database, default to 'public'
        return getattr(self.db, "schema", "public")

    def _qtable(self, table: str) -> str:
        # Construct fully qualified table name with sanitized schema and table
        schema = sanitize_ident(self._schema_name())
        table = sanitize_ident(table)
        return f"{qident(schema)}.{qident(table)}"

    def analyze(self, table_name: str) -> None:
        # Sanitize the input table name
        table_name = sanitize_ident(table_name)

        # Build fully qualified table name
        fq_table = self._qtable(table_name)

        # Execute ANALYZE on the table to update statistics
        with self.db.begin() as conn:
            conn.execute(text(f"ANALYZE {fq_table}"))

        # Log completion
        logger.info(f"ANALYZE completed for <green>{self._schema_name()}.{table_name}</green>")
