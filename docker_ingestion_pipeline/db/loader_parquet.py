from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import ClassVar, Dict, FrozenSet, List

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from docker_ingestion_pipeline.ports.database import Database
from docker_ingestion_pipeline.ports.loader import Loader
from docker_ingestion_pipeline.utils.datetime_fix import fix_datetime_columns
from docker_ingestion_pipeline.utils.identifiers import qident, sanitize_ident
from docker_ingestion_pipeline.utils.progress import track


@dataclass(frozen=True)
class ParquetStreamLoader(Loader):
    """
    Loads Parquet data into Postgres using the COPY command with streaming.

    Key Features:
    - Memory efficient: Reads Parquet in batches instead of loading the full file.
    - Type safe: Coerces Pandas types to match Postgres requirements (handling nullable ints, etc.).
    - Transactional: Commits only if all batches succeed; rolls back on failure.
    """
    db: Database
    batch_size: int = 100_000

    _INT_TYPES: ClassVar[FrozenSet[str]] = frozenset({"bigint", "integer", "smallint"})
    _NUM_TYPES: ClassVar[FrozenSet[str]] = frozenset({"numeric", "decimal", "real", "double precision"})

    def _get_pg_column_types(self, table_name: str, target_cols: list[str]) -> Dict[str, str]:
        pg_types = self.db.get_table_column_types(table_name)
        if not pg_types:
            raise RuntimeError(f"Could not read column types for table '{table_name}'")

        missing_meta = [c for c in target_cols if c not in pg_types]
        if missing_meta:
            raise RuntimeError(f"Missing type metadata for columns in '{table_name}': {missing_meta}")

        return pg_types

    def _coerce_integer_columns(self, df: pd.DataFrame, int_cols: List[str]) -> pd.DataFrame:
        for col in int_cols:
            if col not in df.columns:
                continue

            series = pd.to_numeric(df[col], errors="coerce")

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

    def _coerce_numeric_columns(self, df: pd.DataFrame, num_cols: List[str]) -> pd.DataFrame:
        for col in num_cols:
            if col not in df.columns:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def load(self, file_path: str, table_name: str) -> None:
        table_name = sanitize_ident(table_name)
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

        col_list_sql = ", ".join(qident(c) for c in target_cols)
        copy_sql = (
            f"COPY {qident(table_name)} ({col_list_sql}) "
            "FROM STDIN WITH (FORMAT csv, HEADER false, NULL '', DELIMITER ',')"
        )

        logger.info(f"Starting COPY (Parquet stream) into <green>{table_name}</green> ...")

        pf = pq.ParquetFile(file_path)
        total_rows_est = pf.metadata.num_rows if pf.metadata else None

        raw_conn = self.db.raw_connection()

        try:
            with raw_conn.cursor() as cur:
                total_rows = 0

                with track(total=total_rows_est, desc=f"Loading {table_name}") as tracker:
                    for i, batch in enumerate(pf.iter_batches(batch_size=self.batch_size)):
                        arrow_table = pa.Table.from_batches([batch])

                        if i == 0:
                            existing_cols = set(arrow_table.column_names)
                            missing = [c for c in target_cols if c not in existing_cols]
                            if missing:
                                raise RuntimeError(f"Parquet missing columns: {missing}")

                        arrow_table = arrow_table.select(target_cols)
                        df = arrow_table.to_pandas(integer_object_nulls=True)

                        df = fix_datetime_columns(df)
                        df = self._coerce_numeric_columns(df, cols_to_check_num)
                        df = self._coerce_integer_columns(df, cols_to_check_int)
                        df = df.reindex(columns=target_cols)

                        buf = io.StringIO()
                        df.to_csv(
                            buf,
                            index=False,
                            header=False,
                            na_rep="",
                            lineterminator="\n",
                            quoting=csv.QUOTE_MINIMAL,
                            doublequote=True,
                        )
                        buf.seek(0)

                        cur.copy_expert(copy_sql, buf)

                        rows_loaded = len(df)
                        total_rows += rows_loaded
                        tracker.update(rows_loaded)

                        del df, arrow_table, buf

            raw_conn.commit()
            logger.success(f"COPY finished for <green>{table_name}</green>, rows={total_rows}")

        except Exception:
            raw_conn.rollback()
            logger.exception("Failed to load data, transaction rolled back.")
            raise
        finally:
            raw_conn.close()
