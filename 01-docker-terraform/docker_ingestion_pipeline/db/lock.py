# docker_ingestion_pipeline/db/lock.py

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass

from loguru import logger
from sqlalchemy import text

from docker_ingestion_pipeline.ports.database import Database

@dataclass(frozen=True)
class AdvisoryLock:
    """
    Manages PostgreSQL advisory locks.

    Notes:
    - Uses `pg_advisory_lock` and `pg_advisory_unlock` with a hashed string key.
    - Ensures the lock is released even if an error occurs while the lock is held.
    """
    db: Database

    @contextmanager
    def acquire(self, lock_key: str):
        # Acquire the advisory lock for the given key
        with self.db.begin() as conn:
            conn.execute(
                text("SELECT pg_advisory_lock(hashtext(:k)::bigint)"), {"k": lock_key}
            )

        try:
            # Yield control while the lock is held
            yield

        finally:
            try:
                # Release the advisory lock
                with self.db.begin() as conn:
                    conn.execute(
                        text("SELECT pg_advisory_unlock(hashtext(:k)::bigint)"), {"k": lock_key}
                    )
            except Exception:
                # Warn if releasing the lock fails (might have been already released)
                logger.warning(f"Failed to release advisory lock: {lock_key}, it may already released")
