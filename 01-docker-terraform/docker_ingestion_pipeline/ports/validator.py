# docker-ingestion-pipeline/ports/validator.py

from __future__ import annotations

from typing import Protocol, Optional, Dict, Any


class Validator(Protocol):
    def infer_expected_month_from_table(self, table_name: str) -> Optional[str]: ...

    def validate_staging(
        self,
        staging_table: str,
        expected_month: Optional[str] = None,
    ) -> Dict[str, Any]: ...