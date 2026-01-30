# docker_ingestion_pipeline/config.py

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from loguru import logger


def env_bool(name: str, default: bool = False) -> bool:
    """
    Parses an environment variable as a boolean.
    Returns the default value if the variable is unset.
    True values: "1", "true", "t", "yes", "y", "on" (case-insensitive).
    """
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "t", "yes", "y", "on"}

def env_int(name: str, default: int, *, min_value: int = 1) -> int:
    """
    Parses an environment variable as an integer.
    Returns the default value if the variable is unset.
    """
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        val = int(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer, got={raw!r}") from e
    if val < min_value:
        raise ValueError(f"{name} must be >= {min_value}, got={val}")
    return val



def project_root() -> Path:
    """Returns the absolute path to the project root directory."""
    return Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Paths:
    """Immutable container for project directory paths."""
    base_dir: Path
    data_dir: Path
    log_dir: Path


def build_paths() -> Paths:
    """
    Resolves project paths and creates required directories on disk.
    Defaults data_dir to ./data if DATA_DIR env var is not set.
    """
    base = project_root()

    data_dir = Path(os.getenv("DATA_DIR", str(base / "data")))
    data_dir.mkdir(parents=True, exist_ok=True)

    log_dir = base / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    return Paths(base_dir=base, data_dir=data_dir, log_dir=log_dir)

@dataclass(frozen=True)
class LoaderSettings:
    """Immutable container for loader batching/streaming settings."""
    chunk_size: int
    batch_size: int


def load_loader_settings() -> LoaderSettings:
    """
    Loads loader settings from environment variables.

    - LOADER_CHUNK_SIZE: number of rows per read chunk (streaming from file)
    - LOADER_BATCH_SIZE: number of rows per DB flush/commit batch
    """
    return LoaderSettings(
        chunk_size=env_int("LOADER_CHUNK_SIZE", 50_000, min_value=1),
        batch_size=env_int("LOADER_BATCH_SIZE", 10_000, min_value=1),
    )



def configure_logging(paths: Paths) -> None:
    """Configure loguru sinks (console + file) with tqdm-safe console output."""
    logger.remove()

    try:
        from tqdm import tqdm  # type: ignore
    except Exception:
        tqdm = None

    def _console_sink(message: str) -> None:
        """
        Writes logs to stdout.
        Uses tqdm.write if progress bars are enabled to prevent visual corruption.
        """
        if env_bool("ENABLE_PROGRESS", default=True) and tqdm is not None:
            tqdm.write(message.rstrip("\n"))
        else:
            sys.stdout.write(message)
            sys.stdout.flush()

    # Console sink
    logger.add(
        _console_sink,
        level=os.getenv("LOG_LEVEL", "INFO"),
        colorize=True,
        backtrace=False,
        diagnose=False,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level}</level> | "
               "<level>{message}</level>\n",
    )

    # File sink
    logger.add(
        str(paths.log_dir / "app.log"),
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
        backtrace=False,
        diagnose=False,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
    )
