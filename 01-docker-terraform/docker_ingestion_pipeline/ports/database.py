# docker-ingestion-pipeline/ports/database.py

from __future__ import annotations

from typing import Protocol, List, Any, ContextManager, Mapping, Optional


from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql.elements import TextClause


class Database(Protocol):
    engine: Engine

    def table_exists(self, table: str) -> bool: ...
    def get_table_columns(self, table: str) -> List[str]: ...

    def raw_connection(self) -> Any: ...
    def begin(self) -> ContextManager[Connection]: ...
    def connect(self) -> ContextManager[Connection]: ...

    def execute(
            self,
            stmt: TextClause,
            params: Optional[Mapping[str, Any]] = None,
    ) -> Any: ...

    def get_table_column_types(self, table: str) -> Mapping[str, str]: ...
