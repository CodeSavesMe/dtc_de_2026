from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

from loguru import logger
from sqlalchemy import text

from docker_ingestion_pipeline.ports.database import Database
from docker_ingestion_pipeline.ports.validator import Validator
from docker_ingestion_pipeline.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class PostgresStagingValidator(Validator):
    db: Database

    # Maximum number of rows allowed outside the expected month
    max_outside_month_abs: int = 200

    # Maximum ratio of rows allowed outside the expected month
    max_outside_month_ratio: float = 0.005

    # Whether to raise an error if month validation fails
    fail_on_outside_month: bool = False

    def _detect_datetime_column(self, table_name: str) -> Optional[str]:
        # Sanitize table name to prevent SQL injection
        table_name = sanitize_ident(table_name)

        # Common candidate column names for datetime
        candidates = [
            "lpep_pickup_datetime",
            "tpep_pickup_datetime",
            "pickup_datetime",
        ]

        # Get columns from the database
        columns = self.db.get_table_columns(table_name)

        # Normalize column names for case-insensitive matching
        norm_map = {c.strip().lower(): c for c in columns}

        # Return the first matching datetime column
        for cand in candidates:
            key = cand.strip().lower()
            if key in norm_map:
                return norm_map[key]

        # Log if no datetime column is found
        logger.debug(f"Datetime column not found for {table_name}. Columns: {columns}")
        return None

    def infer_expected_month_from_table(self, table_name: str) -> Optional[str]:
        # Extract the expected year_month from table name (format: YYYY_MM)
        m = re.search(r"(\d{4})_(\d{2})", table_name)
        if m:
            return f"{m.group(1)}_{m.group(2)}"
        return None

    def validate_staging(self, staging_table: str, expected_month: Optional[str] = None) -> Dict[str, Any]:
        # Sanitize table name
        staging_table = sanitize_ident(staging_table)

        # Detect datetime column
        dt_col = self._detect_datetime_column(staging_table)

        # Get total row count from staging table
        with self.db.connect() as conn:
            rowcount = conn.execute(
                text(f"SELECT COUNT(*) FROM {qident(staging_table)}")
            ).scalar_one()

            result: Dict[str, Any] = {"rowcount": rowcount, "datetime_col": dt_col}

            # Get min, max, and null count for datetime column
            if dt_col:
                r = conn.execute(
                    text(
                        f"""
                        SELECT
                          MIN({qident(dt_col)}) AS min_dt,
                          MAX({qident(dt_col)}) AS max_dt,
                          COUNT(*) FILTER (WHERE {qident(dt_col)} IS NULL) AS null_dt
                        FROM {qident(staging_table)}
                        """
                    )
                ).mappings().one()
                result.update({"min_dt": r["min_dt"], "max_dt": r["max_dt"], "null_dt": r["null_dt"]})

        # Log overall validation results
        logger.info(f"Validation result: {result}")

        # Fail if no rows exist in staging
        if rowcount == 0:
            raise RuntimeError("Validation failed: staging table has 0 rows")

        # Validate rows against expected month if provided
        if expected_month and dt_col and rowcount > 0:
            try:
                yyyy, mm = expected_month.split("_")
                start = datetime(int(yyyy), int(mm), 1)
                # Compute end of month (exclusive)
                end = datetime(int(yyyy) + 1, 1, 1) if int(mm) == 12 else datetime(int(yyyy), int(mm) + 1, 1)

                # Count rows inside and outside the expected month
                with self.db.connect() as conn:
                    counts = conn.execute(
                        text(
                            f"""
                            SELECT
                              COUNT(*) FILTER (
                                WHERE {qident(dt_col)} < :start OR {qident(dt_col)} >= :end
                              ) AS outside_month,
                              COUNT(*) FILTER (
                                WHERE {qident(dt_col)} >= :start AND {qident(dt_col)} < :end
                              ) AS inside_month
                            FROM {qident(staging_table)}
                            """
                        ),
                        {"start": start, "end": end},
                    ).mappings().one()

                outside = int(counts["outside_month"])
                inside = int(counts["inside_month"])
                ratio = (outside / rowcount) if rowcount else 0.0

                result.update(
                    {
                        "expected_month": expected_month,
                        "inside_month": inside,
                        "outside_month": outside,
                        "outside_ratio": ratio,
                    }
                )

                # Determine if thresholds are exceeded
                exceeds = (outside > self.max_outside_month_abs) and (ratio > self.max_outside_month_ratio)

                # Raise error or log warning/info based on configuration
                if exceeds:
                    msg = (
                        f"Month validation failed beyond thresholds for {expected_month}: "
                        f"outside={outside}/{rowcount} ({ratio:.4%}). "
                        f"min={result.get('min_dt')}, max={result.get('max_dt')}"
                    )
                    if self.fail_on_outside_month:
                        raise RuntimeError(msg)
                    logger.warning(msg)
                else:
                    logger.info(
                        f"Month spillover within tolerance for {expected_month}: "
                        f"outside={outside}/{rowcount} ({ratio:.4%})."
                    )

            except Exception as e:
                # Log parsing or validation errors but don't break execution
                logger.debug(f"Month validation skipped/failed to parse: {e}")

        return result
