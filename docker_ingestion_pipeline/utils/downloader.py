# docker-ingestion-pipeline/utils/downloader.py

from __future__ import annotations

import os
from urllib.parse import urlparse

import requests
from loguru import logger

from docker_ingestion_pipeline.config import build_paths


def download_file(url: str) -> str:
    """
    Downloads a file from a URL using an atomic write pattern.
    If the file already exists and has size > 0, download is skipped.
    """
    paths = build_paths()

    # Ensure data directory exists
    if not os.path.exists(paths.data_dir):
        os.makedirs(paths.data_dir)

    parsed = urlparse(url)
    file_name = os.path.basename(parsed.path)

    if not file_name:
        raise ValueError("URL is invalid or file name couldn't be determined")

    local_path = os.path.join(str(paths.data_dir), file_name)

    # 1. Check Cache
    # If file exists and is not empty, assume it's valid and skip download
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        logger.info(f"File {file_name} already exists. Skipping download.")
        return local_path

    logger.info(f"Downloading data from {url} -> {local_path}")

    # 2. Atomic Download Pattern
    # Download to a temporary file first (.part)
    temp_path = f"{local_path}.part"

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()

            # Write to temp file
            with open(temp_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        # 3. Rename to final filename (Atomic Operation)
        # This ensures 'local_path' only exists if download finished successfully.
        os.replace(temp_path, local_path)

    except Exception as e:
        # Cleanup temp file if download failed/interrupted
        if os.path.exists(temp_path):
            os.remove(temp_path)
        logger.error(f"Download failed: {e}")
        raise e

    if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
        raise RuntimeError("Download completed but file is missing/empty!")

    return local_path