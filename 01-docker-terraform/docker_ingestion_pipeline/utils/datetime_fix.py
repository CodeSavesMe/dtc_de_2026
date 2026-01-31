# docker-ingestion-pipeline/utils/datetime_fix.py

from __future__ import annotations

import pandas as pd
from loguru import logger

def fix_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:

    datetime_cols = [
        ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
        ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
    ]

    for pickup, dropoff in datetime_cols:
        if pickup in df.columns and dropoff in df.columns:
            logger.debug(F"Converting column {pickup} and {dropoff} to datetime")
            df[pickup] = pd.to_datetime(df[pickup], errors="coerce")
            df[dropoff] = pd.to_datetime(df[dropoff], errors="coerce")

    return df