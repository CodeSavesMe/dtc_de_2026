# docker-ingestion-pipeline/config.py

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from loguru import logger


def project_root() -> Path:
    """
    Return the root directory of the project.

    Notes:
    - Assumes this file is located two levels below the project root.
    """
    return Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Paths:
    # Holds paths to key project directories
    base_dir: Path
    data_dir: Path
    log_dir: Path


def build_paths() -> Paths:
    # Determine project root
    base = project_root()

    # Create or get data directory from environment variable or default
    data_dir = Path(os.getenv("DATA_DIR", str(base / "data")))
    data_dir.mkdir(parents=True, exist_ok=True)

    # Create logs directory under project root
    log_dir = base / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    return Paths(base_dir=base, data_dir=data_dir, log_dir=log_dir)


def configure_logging(paths: Paths) -> None:
    # Remove default logger configuration
    logger.remove()

    # Log to console with basic formatting and INFO level by default
    logger.add(
        sys.stdout,
        level=os.getenv("LOG_LEVEL", "INFO"),
        colorize=True,
        backtrace=False,
        diagnose=False,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level}</level> | "
               "<level>{message}</level>",
    )

    # Log to file with detailed formatting and DEBUG level
    logger.add(
        str(paths.log_dir / "app.log"),
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
    )
