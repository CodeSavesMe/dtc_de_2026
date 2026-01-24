# docker_ingestion_pipeline/db/loader_tsv.py

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import ClassVar, Dict, FrozenSet, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from docker_ingestion_pipeline.ports.database import Database
from docker_ingestion_pipeline.ports.loader import Loader
from docker_ingestion_pipeline.utils.datetime_fix import fix_datetime_columns
from docker_ingestion_pipeline.utils.identifiers import qident, sanitize_ident
from docker_ingestion_pipeline.utils.progress import track, progress_enable


@dataclass(frozen=True)
class TsvCopyLoader(Loader):
    """
    Loads TSV data into Postgres using batches.

    - Reads input as TSV (tab-separated).
    - Converts chunks to standard CSV format in memory.
    - Streams to Postgres using COPY.
    """
    db: Database
    chunksize: int = 100_000
    encoding: str = "utf-8"
    delimiter: str = "\t"  # Input file uses tabs

    _INT_TYPES: ClassVar[FrozenSet[str]] = frozenset({"bigint", "integer", "smallint"})
    _NUM_TYPES: ClassVar[FrozenSet[str]] = frozenset({"numeric", "decimal", "real", "double precision"})

    def _get_pg_column_types(self, table_name: str, target_cols: list[str]) -> Dict[str, str]:
        """Gets column types from the database."""
        pg_types = self.db.get_table_column_types(table_name)
        if not pg_types:
            raise RuntimeError(f"Could not read column types for table '{table_name}'")

        missing_meta = [c for c in target_cols if c not in pg_types]
        if missing_meta:
            raise RuntimeError(f"Missing type metadata for columns in '{table_name}': {missing_meta}")

        return pg_types

    def _coerce_numeric_columns(self, df: pd.DataFrame, num_cols: List[str]) -> pd.DataFrame:
        """Converts columns to numbers. Invalid values become NaN."""
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def _coerce_integer_columns(self, df: pd.DataFrame, int_cols: List[str]) -> pd.DataFrame:
        """
        Fixes integer columns.
        - Converts valid numbers to integers.
        - Sets values with decimals (like 1.5) to NULL.
        """
        for col in int_cols:
            if col not in df.columns:
                continue

            series = pd.to_numeric(df[col], errors="coerce")

            # Check for decimals
            frac = (series % 1).abs()
            has_decimals = series.notna() & (~np.isclose(frac, 0.0, atol=1e-9))

            if has_decimals.any():
                example = series[has_decimals].iloc[0]
                logger.warning(
                    f"Non-integer values found in integer column '{col}'. "
                    f"Setting to NULL. Example={example}"
                )
                series = series.mask(has_decimals, np.nan)

            df[col] = series.astype("Int64")

        return df

    def load(self, file_path: str, table_name: str) -> None:
        table_name = sanitize_ident(table_name)

        # 1. Get database schema
        target_cols = self.db.get_table_columns(table_name)
        if not target_cols:
            raise RuntimeError(f"Target table {table_name} has no columns")

        target_col_set = set(target_cols)
        pg_types = self._get_pg_column_types(table_name, target_cols)

        cols_to_check_int = [
            col for col, dtype in pg_types.items()
            if dtype in self._INT_TYPES and col in target_col_set
        ]
        cols_to_check_num = [
            col for col, dtype in pg_types.items()
            if dtype in self._NUM_TYPES and col in target_col_set
        ]

        # 2. Prepare SQL
        # Use standard CSV format (comma) for the database stream to ensure stability.
        col_list_sql = ", ".join(qident(c) for c in target_cols)
        copy_sql = (
            f"COPY {qident(table_name)} ({col_list_sql}) "
            "FROM STDIN WITH (FORMAT csv, HEADER false, NULL '', DELIMITER ',')"
        )

        logger.info(f"Starting COPY (TSV chunks) into <green>{table_name}</green> ...")

        total_est: Optional[int] = None
        raw_conn = self.db.raw_connection()

        try:
            with raw_conn.cursor() as cur:
                total_rows = 0

                with track(total=total_est, desc=f"Loading {table_name}") as tracker:
                    # 3. Read input file
                    reader = pd.read_csv(
                        file_path,
                        sep=self.delimiter,
                        chunksize=self.chunksize,
                        usecols=lambda c: c.strip() in target_col_set,
                        dtype=str,
                        compression="infer",
                        encoding=self.encoding,
                        low_memory=False,
                    )

                    for i, chunk in enumerate(reader):
                        # Remove spaces from headers in every chunk
                        chunk.columns = [c.strip() for c in chunk.columns]

                        # Check for duplicates or missing columns on the first chunk
                        if i == 0:
                            if len(chunk.columns) != len(set(chunk.columns)):
                                raise RuntimeError(
                                    "Duplicate columns detected after stripping TSV header"
                                )

                            missing = [c for c in target_cols if c not in chunk.columns]
                            if missing:
                                raise RuntimeError(
                                    f"TSV missing columns required by target table: {missing}"
                                )

                        # Match column order with database
                        chunk = chunk.reindex(columns=target_cols)

                        # Fix data types
                        chunk = fix_datetime_columns(chunk)
                        chunk = self._coerce_numeric_columns(chunk, cols_to_check_num)
                        chunk = self._coerce_integer_columns(chunk, cols_to_check_int)

                        # Write to buffer using standard CSV delimiter (comma)
                        buf = io.StringIO()
                        chunk.to_csv(
                            buf,
                            index=False,
                            header=False,
                            sep=",",
                            na_rep="",
                            lineterminator="\n",
                            quoting=csv.QUOTE_MINIMAL,
                            doublequote=True,
                        )
                        buf.seek(0)

                        # Send to database
                        cur.copy_expert(copy_sql, buf)

                        rows_loaded = len(chunk)
                        total_rows += rows_loaded
                        tracker.update(rows_loaded)

                        if (i + 1) % 10 == 0 and not progress_enable():
                            logger.debug(
                                f"Streamed {i + 1} TSV chunks (rows so far={total_rows})..."
                            )

            raw_conn.commit()
            logger.success(f"COPY finished for <green>{table_name}</green>, rows={total_rows}")

        except Exception:
            raw_conn.rollback()
            logger.exception("Failed to load TSV data")
            raise
        finally:
            raw_conn.close()