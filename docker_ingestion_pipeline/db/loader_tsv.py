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
    - Streams to Postgres using COPY for maximum performance.
    """
    db: Database
    chunk_size: int = 100_000
    batch_size: int = 10_000
    encoding: str = "utf-8"
    delimiter: str = "\t"  # Input file uses tabs

    # PostgreSQL type categories for mapping
    _INT_TYPES: ClassVar[FrozenSet[str]] = frozenset({"bigint", "integer", "smallint"})
    _NUM_TYPES: ClassVar[FrozenSet[str]] = frozenset({"numeric", "decimal", "real", "double precision"})

    def _get_pg_column_types(self, table_name: str, target_cols: list[str]) -> Dict[str, str]:
        """Fetch existing column data types from the target database."""
        pg_types = self.db.get_table_column_types(table_name)
        if not pg_types:
            raise RuntimeError(f"Could not read column types for table '{table_name}'")

        missing_meta = [c for c in target_cols if c not in pg_types]
        if missing_meta:
            raise RuntimeError(f"Missing type metadata for columns in '{table_name}': {missing_meta}")

        return pg_types

    def _coerce_numeric_columns(self, df: pd.DataFrame, num_cols: List[str]) -> pd.DataFrame:
        """Force conversion to numeric; invalid strings become NaN (NULL)."""
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def _coerce_integer_columns(self, df: pd.DataFrame, int_cols: List[str]) -> pd.DataFrame:
        """
        Clean integer columns:
        1. Parse to numeric.
        2. Replace actual decimals (e.g., 1.5) with NULL to prevent DB errors.
        3. Convert to nullable Int64.
        """
        for col in int_cols:
            if col not in df.columns:
                continue

            series = pd.to_numeric(df[col], errors="coerce")

            # Identify rows with actual decimal fractions
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
        """Main ingestion logic: Read TSV -> Process in Pandas -> Copy to Postgres."""
        table_name = sanitize_ident(table_name)

        # 1. Fetch Schema Metadata
        target_cols = self.db.get_table_columns(table_name)
        if not target_cols:
            raise RuntimeError(f"Target table {table_name} has no columns")

        target_col_set = set(target_cols)
        pg_types = self._get_pg_column_types(table_name, target_cols)

        # Categorize columns for specialized cleaning
        cols_to_check_int = [col for col, dtype in pg_types.items() if dtype in self._INT_TYPES and col in target_col_set]
        cols_to_check_num = [col for col, dtype in pg_types.items() if dtype in self._NUM_TYPES and col in target_col_set]

        # 2. Prepare SQL
        # We stream as CSV (comma) even if the source is TSV to ensure stable parsing in Postgres
        col_list_sql = ", ".join(qident(c) for c in target_cols)
        copy_sql = (
            f"COPY {qident(table_name)} ({col_list_sql}) "
            "FROM STDIN WITH (FORMAT csv, HEADER false, NULL '', DELIMITER ',')"
        )

        logger.info(f"Starting COPY (TSV chunks) into <green>{table_name}</green> ...")

        raw_conn = self.db.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                total_rows = 0

                with track(total=None, desc=f"Loading {table_name}") as tracker:
                    # 3. Stream read from TSV file in chunks to save memory
                    reader = pd.read_csv(
                        file_path,
                        sep=self.delimiter,
                        chunksize=self.chunk_size,
                        usecols=lambda c: c.strip() in target_col_set,
                        dtype=str,
                        compression="infer",
                        encoding=self.encoding,
                        low_memory=False,
                    )

                    for i, chunk in enumerate(reader):
                        # Sanitize headers
                        chunk.columns = [c.strip() for c in chunk.columns]

                        # Validation for the first chunk
                        if i == 0:
                            if len(chunk.columns) != len(set(chunk.columns)):
                                raise RuntimeError("Duplicate columns detected in TSV header")
                            missing = [c for c in target_cols if c not in chunk.columns]
                            if missing:
                                raise RuntimeError(f"TSV missing columns: {missing}")

                        # 4. Data Transformation
                        chunk = chunk.reindex(columns=target_cols)
                        chunk = fix_datetime_columns(chunk)
                        chunk = self._coerce_numeric_columns(chunk, cols_to_check_num)
                        chunk = self._coerce_integer_columns(chunk, cols_to_check_int)

                        # 5. Internal Batching: Convert chunk to CSV and send to DB
                        rows_in_chunk = len(chunk)
                        for start in range(0, rows_in_chunk, self.batch_size):
                            batch = chunk.iloc[start:start + self.batch_size]

                            buf = io.StringIO()
                            batch.to_csv(
                                buf,
                                index=False,
                                header=False,
                                sep=",",  # Transform TSV data to CSV format for the stream
                                na_rep="",
                                lineterminator="\n",
                                quoting=csv.QUOTE_MINIMAL,
                                doublequote=True,
                            )
                            buf.seek(0)
                            cur.copy_expert(copy_sql, buf)

                            rows_loaded = len(batch)
                            total_rows += rows_loaded
                            tracker.update(rows_loaded)

                            # Explicitly clean up small batch buffers
                            del batch, buf

                        # Heartbeat logging for non-interactive logs (e.g., Docker/CI)
                        if (i + 1) % 5 == 0 and not progress_enable():
                            logger.info(f"Processed {i + 1} chunks... Total rows: {total_rows}")

                        # Explicitly clean up the large chunk
                        del chunk

            raw_conn.commit()
            logger.success(f"TSV Load Completed: {table_name} ({total_rows} rows)")

        except Exception:
            raw_conn.rollback()
            logger.exception("Failed to load TSV data")
            raise
        finally:
            raw_conn.close()