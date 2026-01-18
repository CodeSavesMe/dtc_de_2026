# docker-ingestion-pipeline/ports/loader.py

from typing import Protocol


class Loader(Protocol):
    def load(self, file_path: str, table_name: str) -> None: ...