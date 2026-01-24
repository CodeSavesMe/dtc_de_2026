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

    Capabilities:
    - Infers schema from CSV, TSV, and Parquet files.
    - Bootstraps final tables and creates staging tables.
    - Handles dialect differences (e.g., separators) for flat files.
    """
    db: Database
    sample_rows: int = 2000

    def ensure_final_schema(self, file_path: str, final_table: str) -> None:
        """
        Ensures the final destination table exists with the correct schema.
        Detects file format and delegates to the appropriate bootstrapping method.
        """
        # Sanitize table name to prevent SQL injection or syntax errors
        final_table = sanitize_ident(final_table)

        # Detect file format (CSV, TSV, Parquet, etc.)
        fmt = detect_file_format(file_path)

        if fmt == FileFormat.PARQUET:
            # Parquet files contain schema metadata, so we read the footer
            self._bootstrap_final_schema_from_parquet_footer(file_path, final_table)
            return

        if fmt in (FileFormat.CSV, FileFormat.TSV):
            # For CSV/TSV, we infer schema from sample rows.
            # CRITICAL: We pass 'fmt' to ensure the correct delimiter is used.
            self._bootstrap_final_schema_from_csv_sample(
                file_path, final_table, sample_rows=self.sample_rows, fmt=fmt
            )
            return

        # Raise error if format is not supported
        raise ValueError(f"Unsupported file format for schema bootstrap: {file_path} ({fmt})")

    def recreate_staging_like_final(self, final_table: str, staging_table: str) -> None:
        """
        Drops the staging table (if exists) and creates a new empty one
        mirroring the structure of the final table.
        """
        final_table = sanitize_ident(final_table)
        staging_table = sanitize_ident(staging_table)

        with self.db.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {qident(staging_table)}"))
            # 'INCLUDING ALL' ensures indexes and constraints are copied if needed
            conn.execute(text(f"CREATE TABLE {qident(staging_table)} (LIKE {qident(final_table)} INCLUDING ALL)"))

        logger.info(f"Created staging <green>{staging_table}</green> LIKE <green>{final_table}</green>")

    def _bootstrap_final_schema_from_csv_sample(
            self,
            file_path: str,
            final_table: str,
            sample_rows: int,
            fmt: FileFormat = FileFormat.CSV
    ) -> None:
        """
        Infers schema from CSV or TSV sample rows.

        Args:
            fmt: The detected file format. This determines the separator
                 (',' for CSV, '\t' for TSV) to prevent parsing errors.
        """
        logger.info(f"Bootstrapping schema for <green>{final_table}</green> from {fmt.name} sample rows={sample_rows}")

        # specific separator logic to avoid "single column" inference errors
        separator = "\t" if fmt == FileFormat.TSV else ","

        # Read a sample using the correct separator
        df = pd.read_csv(
            file_path,
            nrows=sample_rows,
            compression="infer",
            low_memory=False,
            sep=separator  # <--- Ensures columns are split correctly
        )

        # Standardize datetime formats before SQL creation
        df = fix_datetime_columns(df)

        # Create table using pandas' SQL utility (creates table based on DataFrame dtypes)
        df.head(0).to_sql(name=final_table, con=self.db.engine, if_exists="replace", index=False)

        logger.info(f"Schema bootstrapped for <green>{final_table}</green>")

    def _arrow_type_to_pg(self, arrow_type: pa.DataType) -> str:
        """
        Maps PyArrow data types (from Parquet) to PostgreSQL data types.
        """
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
        """
        Creates a table schema by reading the metadata footer of a Parquet file.
        This is faster and more accurate than sampling rows for Parquet.
        """
        logger.info(f"Bootstrapping schema for <green>{final_table}</green> from Parquet footer")

        pf = pq.ParquetFile(file_path)
        schema = pf.schema_arrow

        cols_sql = []
        for field in schema:
            col = sanitize_ident(field.name)
            pg_type = self._arrow_type_to_pg(field.type)
            cols_sql.append(f"{qident(col)} {pg_type}")

        ddl = f"CREATE TABLE {qident(final_table)} ({', '.join(cols_sql)})"

        with self.db.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {qident(final_table)}"))
            conn.execute(text(ddl))

        logger.info(f"Schema bootstrapped for <green>{final_table}</green>")