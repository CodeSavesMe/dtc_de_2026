# docker_ingestion_pipeline/db/lock.py

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass

from loguru import logger
from sqlalchemy import text

from docker_ingestion_pipeline.ports.database import Database


@dataclass(frozen=True)
class AdvisoryLock:
    """
    Manages PostgreSQL advisory locks using a polling mechanism.

    Ensures the database connection remains open (session-scoped) while the lock
    is held, preventing accidental releases or race conditions.
    """
    db: Database

    @contextmanager
    def acquire(
            self, lock_key: str, *, timeout_s: float | None = 60.0, poll_s: float = 0.2
    ):
        """
        Acquires a lock, polling until success or timeout.
        """
        # Maintain a persistent connection for the duration of the lock (Session Scope)
        with self.db.connect() as conn:
            lock_sql = text("SELECT pg_try_advisory_lock(hashtext(:k)::bigint)")
            unlock_sql = text("SELECT pg_advisory_unlock(hashtext(:k)::bigint)")


            start_time = time.monotonic()

            # Polling loop: Try to acquire lock non-blockingly
            while True:
                is_acquired = bool(conn.execute(lock_sql, {"k": lock_key}).scalar())

                if is_acquired:
                    break

                # Check for timeout
                if timeout_s and (time.monotonic() - start_time) >= timeout_s:
                    raise TimeoutError(f"Failed to acquire lock '{lock_key}' after {timeout_s}s")

                time.sleep(poll_s)

            try:
                logger.info(f"Advisory lock acquired: {lock_key}")
                yield
            finally:
                # Always release the lock using the same connection
                try:
                    conn.execute(unlock_sql, {"k": lock_key})
                    logger.info(f"Advisory lock released: {lock_key}")
                except Exception as e:
                    logger.warning(f"Failed to release lock '{lock_key}': {e}")