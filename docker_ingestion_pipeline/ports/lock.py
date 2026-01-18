# docker-ingestion-pipeline/ports/lock.py

from __future__ import annotations

from typing import Protocol, ContextManager


class LockManager(Protocol):
    def acquire(self, lock_key: str) -> ContextManager[None]: ...