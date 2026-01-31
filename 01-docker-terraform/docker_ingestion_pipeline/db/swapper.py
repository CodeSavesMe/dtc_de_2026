# docker_ingestion_pipeline/db/swapper.py

from __future__ import annotations

from dataclasses import dataclass

from loguru import logger
from sqlalchemy import text

from docker_ingestion_pipeline.ports.database import Database
from docker_ingestion_pipeline.ports.swapper import Swapper
from docker_ingestion_pipeline.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class AtomicSwapper(Swapper):
    db: Database

    def _schema_name(self) -> str:
        return getattr(self.db, "schema", "public")

    def _qtable(self, table: str) -> str:
        schema = sanitize_ident(self._schema_name())
        table = sanitize_ident(table)
        return f"{qident(schema)}.{qident(table)}"

    def swap_tables_atomically(self, final_table: str, staging_table: str) -> None:
        final_table = sanitize_ident(final_table)
        staging_table = sanitize_ident(staging_table)
        backup_table = sanitize_ident(f"{final_table}__backup")

        schema = sanitize_ident(self._schema_name())

        fq_final = self._qtable(final_table)
        fq_staging = self._qtable(staging_table)
        fq_backup = self._qtable(backup_table)

        # for to_regclass() we pass an unquoted regclass text like: schema.table
        reg_final = f"{schema}.{final_table}"

        logger.info(
            f"Swapping atomically (schema={schema}): "
            f"staging=<green>{staging_table}</green> -> final=<green>{final_table}</green>"
        )

        with self.db.begin() as conn:
            # cleanup any leftover backup
            conn.execute(text(f"DROP TABLE IF EXISTS {fq_backup}"))

            # check existence safely
            exists = conn.execute(
                text("SELECT to_regclass(:t) IS NOT NULL"),
                {"t": reg_final},
            ).scalar_one()

            if exists:
                # rename final -> backup (stays in same schema)
                conn.execute(text(f"ALTER TABLE {fq_final} RENAME TO {qident(backup_table)}"))

            # rename staging -> final (stays in same schema)
            conn.execute(text(f"ALTER TABLE {fq_staging} RENAME TO {qident(final_table)}"))

            # drop backup after successful swap
            conn.execute(text(f"DROP TABLE IF EXISTS {fq_backup}"))

        logger.success(f"Swap completed: <green>{schema}.{final_table}</green> updated")
