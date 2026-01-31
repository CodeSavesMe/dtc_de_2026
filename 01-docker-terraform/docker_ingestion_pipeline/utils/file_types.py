# docker-ingestion-pipeline/utils/file_types.py

from __future__ import annotations
from enum import Enum


class FileFormat(str, Enum):
    CSV = "csv"
    TSV = "tsv"
    PARQUET = "parquet"
    UNKNOWN = "unknown"

def detect_file_format(path: str) -> FileFormat:
    p = path.lower()

    if p.endswith(".parquet"):
        return FileFormat.PARQUET
    if p.endswith(".csv") or p.endswith(".csv.gz"):
        return FileFormat.CSV
    if p.endswith(".tsv") or p.endswith(".tsv.gz"):
        return FileFormat.TSV

    return FileFormat.UNKNOWN