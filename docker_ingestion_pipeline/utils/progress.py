from __future__ import annotations

import math
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from docker_ingestion_pipeline.config import env_bool


def progress_enable() -> bool:
    """Check if progress bars are enabled via ENABLE_PROGRESS. Returns True by default."""
    return env_bool("ENABLE_PROGRESS", default=True)


def ceil_divide(total_items: int, items_per_batch: int) -> int:
    if items_per_batch <= 0:
        raise ValueError("items_per_batch must be positive")
    return int(math.ceil(total_items / items_per_batch))


@dataclass
class NoopProgress:
    def update(self, n: int) -> None:
        return


@contextmanager
def track(*, total: Optional[int], desc: str):
    if (not progress_enable()) or (tqdm is None):
        yield NoopProgress()
        return

    bar = tqdm(total=total, desc=desc, leave=False, dynamic_ncols=True)
    try:
        yield bar
    finally:
        bar.close()
