# docker_ingestion_pipeline/db/loader_parquet.py

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import Dict

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from docker_ingestion_pipeline.ports.database import Database
from docker_ingestion_pipeline.ports.loader import Loader
from docker_ingestion_pipeline.utils.identifiers import sanitize_ident, qident
from docker_ingestion_pipeline.utils.datetime_fix import fix_datetime_columns


@dataclass(frozen=True)
class ParquetStreamLoader(Loader):
    """
    Load Parquet data into Postgres using COPY.

    - Reads parquet in batches to limit memory usage
    - Column order always follows the target Postgres table
    - Cleans values that would cause COPY to fail
    - Empty CSV fields are interpreted as SQL NULL
    """
    db: Database
    batch_size: int = 100_000

    def _get_pg_column_types(self, table_name: str, target_cols: list[str]) -> Dict[str, str]:
        """
        Read column data types from Postgres.
        Used to decide coercion rules before COPY.
        """
        pg_types = self.db.get_table_column_types(table_name)

        if not pg_types:
            raise RuntimeError(f"Could not read column types for table '{table_name}'")

        missing_meta = [c for c in target_cols if c not in pg_types]
        if missing_meta:
            raise RuntimeError(
                f"Missing type metadata for columns in '{table_name}': {missing_meta}"
            )

        return pg_types

    def _coerce_integer_columns(self, df: pd.DataFrame, pg_types: Dict[str, str]) -> pd.DataFrame:
        """
        Prepare integer columns so COPY does not fail.

        - 1.0 -> 1
        - Fractional values -> NULL
        """
        int_types = {"bigint", "integer", "smallint"}

        for col, dtype in pg_types.items():
            if col not in df.columns or dtype not in int_types:
                continue

            s = pd.to_numeric(df[col], errors="coerce")

            frac = (s % 1).abs()
            bad = (~s.isna()) & (~np.isclose(frac, 0.0, atol=1e-9))

            if bad.any():
                example = s[bad].iloc[0]
                logger.warning(
                    f"Non-integer values found in integer column '{col}'. "
                    f"Setting to NULL. Example={example}"
                )
                s = s.mask(bad, np.nan)

            df[col] = s.astype("Int64")

        return df

    def _coerce_numeric_columns(self, df: pd.DataFrame, pg_types: Dict[str, str]) -> pd.DataFrame:
        """
        Convert numeric columns to numbers.
        Invalid values become NULL.
        """
        num_types = {"numeric", "decimal", "real", "double precision"}

        for col, dtype in pg_types.items():
            if col not in df.columns or dtype not in num_types:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def load(self, file_path: str, table_name: str) -> None:
        """
        Stream parquet data into Postgres using COPY FROM STDIN.
        """
        table_name = sanitize_ident(table_name)

        target_cols = self.db.get_table_columns(table_name)
        if not target_cols:
            raise RuntimeError(f"Target table {table_name} has no columns")

        pg_types = self._get_pg_column_types(table_name, target_cols)

        col_list_sql = ", ".join(qident(c) for c in target_cols)
        copy_sql = (
            f"COPY {qident(table_name)} ({col_list_sql}) "
            "FROM STDIN WITH (FORMAT csv, HEADER false, NULL '')"
        )

        logger.info(f"Starting COPY (Parquet stream) into <green>{table_name}</green> ...")

        pf = pq.ParquetFile(file_path)
        raw_conn = self.db.raw_connection()

        try:
            with raw_conn.cursor() as cur:
                total_rows = 0

                for i, batch in enumerate(pf.iter_batches(batch_size=self.batch_size)):
                    tbl = pa.Table.from_batches([batch])

                    missing = [c for c in target_cols if c not in tbl.column_names]
                    if missing:
                        raise RuntimeError(
                            f"Parquet missing columns required by target table: {missing}"
                        )

                    tbl = tbl.select(target_cols)

                    df = tbl.to_pandas(integer_object_nulls=True)

                    df = fix_datetime_columns(df)
                    df = self._coerce_integer_columns(df, pg_types)
                    df = self._coerce_numeric_columns(df, pg_types)

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

                    total_rows += len(df)
                    if (i + 1) % 10 == 0:
                        logger.debug(
                            f"Streamed {i+1} parquet batches (rows so far={total_rows})..."
                        )

            raw_conn.commit()
            logger.success(
                f"COPY finished for <green>{table_name}</green>, total rows={total_rows}"
            )

        except Exception:
            raw_conn.rollback()
            raise

        finally:
            raw_conn.close()
