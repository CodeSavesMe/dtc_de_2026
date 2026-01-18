# docker-ingestion-pipeline/utils/identifiers.py

import re

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sanitize_ident(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name

def qident(name: str) -> str:
    return f'"{sanitize_ident(name)}"'