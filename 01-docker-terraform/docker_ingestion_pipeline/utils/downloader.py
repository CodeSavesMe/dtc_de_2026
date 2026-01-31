# docker-ingestion-pipeline/utils/downloader.py

from __future__ import annotations

import os
from urllib.parse import urlparse

import requests
from loguru import logger

from docker_ingestion_pipeline.config import build_paths

def download_file(url: str) -> str:
    paths = build_paths()

    parsed = urlparse(url)
    file_name = os.path.basename(parsed.path)

    if not file_name:
        raise ValueError("URL is invalid or file name could'nt be determined")

    # Init checking for local file, skipped if already exist
    local_path = os.path.join(str(paths.data_dir), file_name)

    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        logger.info(f"File {file_name} already exists, skipping download")
        return local_path

    logger.info(f"Downloading data from {url} -> {local_path}")

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk: f.write(chunk)

    if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
        raise RuntimeError("Download completed but file is missing/empty!")

    return local_path