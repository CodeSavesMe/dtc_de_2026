# docker_ingestion_pipeline/db/client.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ContextManager, List, Dict, Mapping, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL, Connection
from sqlalchemy.sql.elements import TextClause

from docker_ingestion_pipeline.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class PostgresClient:
    """
    Small wrapper around a SQLAlchemy Postgres engine.

    Purpose:
    - Create and configure the engine
    - Keep schema handling consistent
    - Provide helpers for table and column metadata
    - Expose raw psycopg2 connections for COPY
    """

    engine: Engine
    schema: str = "public"

    @classmethod
    def from_params(
        cls,
        user: str,
        password: str,
        host: str,
        port: int,
        db: str,
        schema: str = "public",
    ) -> "PostgresClient":
        """
        Build a PostgresClient from connection parameters.
        """

        schema = schema.strip()
        if not schema:
            raise ValueError("schema must be a non-empty string")

        # Build database URL (handles special characters safely)
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=db,
        )

        engine = create_engine(
            url,
            pool_pre_ping=True,   # reconnect if connection is stale
            future=True,
            connect_args={
                # Set search_path for every connection, including raw psycopg2 ones.
                # 'public' is kept as a fallback.
                "options": f"-csearch_path={schema},public"
            },
        )

        client = cls(engine=engine, schema=schema)
        client.ensure_schema_exists()
        return client

    def ensure_schema_exists(self) -> None:
        """
        Create schema if it does not exist.
        """
        schema = sanitize_ident(self.schema)
        with self.engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {qident(schema)}"))

    # ------------------------------------------------------------------

    def execute(
        self,
        stmt: TextClause,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        """
        Execute a statement using a short-lived connection.
        """
        with self.engine.connect() as conn:
            return conn.execute(stmt, params or {})

    def table_exists(self, table: str) -> bool:
        """
        Check whether a table exists in the configured schema.
        """
        table = sanitize_ident(table)
        with self.engine.connect() as conn:
            return bool(
                conn.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = :s
                              AND table_name   = :t
                        )
                        """
                    ),
                    {"s": self.schema, "t": table},
                ).scalar_one()
            )

    def get_table_columns(self, table: str) -> List[str]:
        """
        Return column names in database order.
        """
        table = sanitize_ident(table)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = :s
                      AND table_name   = :t
                    ORDER BY ordinal_position
                    """
                ),
                {"s": self.schema, "t": table},
            ).fetchall()
        return [r[0] for r in rows]

    def get_table_column_types(self, table: str) -> Dict[str, str]:
        """
        Return column data types from information_schema.

        Example:
        {
            "id": "bigint",
            "amount": "numeric",
            "created_at": "timestamp without time zone"
        }
        """
        table = sanitize_ident(table)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = :s
                      AND table_name   = :t
                    ORDER BY ordinal_position
                    """
                ),
                {"s": self.schema, "t": table},
            ).fetchall()
        return {col: dtype for col, dtype in rows}

    def raw_connection(self) -> Any:
        """
        Return a psycopg2 connection (used by COPY).
        """
        return self.engine.raw_connection()

    def begin(self) -> ContextManager[Connection]:
        """
        Transaction context manager.
        """
        return self.engine.begin()

    def connect(self) -> ContextManager[Connection]:
        """
        Connection context manager.
        """
        return self.engine.connect()
