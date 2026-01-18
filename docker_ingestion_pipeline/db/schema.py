# docker_ingestion_pipeline/db/schema.py

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from loguru import logger
from sqlalchemy import text

import pyarrow.parquet as pq
import pyarrow as pa

from docker_ingestion_pipeline.ports.database import Database
from docker_ingestion_pipeline.ports.schema import SchemaManager
from docker_ingestion_pipeline.utils.identifiers import sanitize_ident, qident
from docker_ingestion_pipeline.utils.datetime_fix import fix_datetime_columns
from docker_ingestion_pipeline.utils.file_types import detect_file_format, FileFormat


@dataclass(frozen=True)
class PostgresSchemaManager(SchemaManager):
    """
    Manages PostgreSQL table schemas based on input files.

    Notes:
    - Can infer schema from CSV, TSV, or Parquet files.
    - Supports bootstrapping the final table schema and creating staging tables.
    - Uses sanitized identifiers to prevent SQL issues.
    """
    db: Database
    sample_rows: int = 2000

    def ensure_final_schema(self, file_path: str, final_table: str) -> None:
        # Sanitize table name
        final_table = sanitize_ident(final_table)

        # Detect file format
        fmt = detect_file_format(file_path)

        if fmt == FileFormat.PARQUET:
            # Build schema using Parquet file footer
            self._bootstrap_final_schema_from_parquet_footer(file_path, final_table)
            return

        if fmt in (FileFormat.CSV, FileFormat.TSV):
            # Treat TSV as CSV for schema inference using sample rows
            self._bootstrap_final_schema_from_csv_sample(
                file_path, final_table, sample_rows=self.sample_rows
            )
            return

        # Raise error if format is not supported
        raise ValueError(f"Unsupported file format for schema bootstrap: {file_path} ({fmt})")

    def recreate_staging_like_final(self, final_table: str, staging_table: str) -> None:
        # Sanitize table names
        final_table = sanitize_ident(final_table)
        staging_table = sanitize_ident(staging_table)

        # Drop staging table if it exists and create a new one like the final table
        with self.db.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {qident(staging_table)}"))
            conn.execute(text(f"CREATE TABLE {qident(staging_table)} (LIKE {qident(final_table)} INCLUDING ALL)"))

        # Log completion
        logger.info(f"Created staging <green>{staging_table}</green> LIKE <green>{final_table}</green>")

    def _bootstrap_final_schema_from_csv_sample(self, file_path: str, final_table: str, sample_rows: int) -> None:
        # Log start of schema bootstrapping
        logger.info(f"Bootstrapping schema for <green>{final_table}</green> from CSV sample rows={sample_rows}")

        # Read a sample of the CSV and fix datetime columns
        df = pd.read_csv(file_path, nrows=sample_rows, compression="infer", low_memory=False)
        df = fix_datetime_columns(df)

        # Create table with inferred schema based on sample
        df.head(0).to_sql(name=final_table, con=self.db.engine, if_exists="replace", index=False)

        # Log completion
        logger.info(f"Schema bootstrapped for <green>{final_table}</green>")

    def _arrow_type_to_pg(self, arrow_type: pa.DataType) -> str:
        # Map Arrow type to PostgreSQL type
        t = str(arrow_type)

        if pa.types.is_timestamp(arrow_type):
            if getattr(arrow_type, "tz", None):
                return "TIMESTAMPTZ"
            return "TIMESTAMP"

        if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
            return "SMALLINT"
        if pa.types.is_int32(arrow_type):
            return "INTEGER"
        if pa.types.is_int64(arrow_type):
            return "BIGINT"

        if pa.types.is_float32(arrow_type):
            return "REAL"
        if pa.types.is_float64(arrow_type):
            return "DOUBLE PRECISION"

        if pa.types.is_boolean(arrow_type):
            return "BOOLEAN"

        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "TEXT"

        if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
            return "DATE"

        if pa.types.is_decimal(arrow_type):
            return f"NUMERIC({arrow_type.precision},{arrow_type.scale})"

        if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return "BYTEA"

        # Fallback type
        logger.debug(f"Arrow type fallback to TEXT: {t}")
        return "TEXT"

    def _bootstrap_final_schema_from_parquet_footer(self, file_path: str, final_table: str) -> None:
        # Log start of schema bootstrapping
        logger.info(f"Bootstrapping schema for <green>{final_table}</green> from Parquet footer")

        # Read Parquet file schema
        pf = pq.ParquetFile(file_path)
        schema = pf.schema_arrow

        # Build column definitions
        cols_sql = []
        for field in schema:
            col = sanitize_ident(field.name)
            pg_type = self._arrow_type_to_pg(field.type)
            cols_sql.append(f"{qident(col)} {pg_type}")

        # Build CREATE TABLE statement
        ddl = f"CREATE TABLE {qident(final_table)} ({', '.join(cols_sql)})"

        # Drop existing table and create new one
        with self.db.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {qident(final_table)}"))
            conn.execute(text(ddl))

        # Log completion
        logger.info(f"Schema bootstrapped for <green>{final_table}</green>")
