# 1. Base Image: Use a lightweight Python 3.13 image
FROM python:3.13-slim-bookworm

# 2. Tooling: Copy 'uv' binary (faster than installing)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# 3. Work Directory: Set workspace inside container
WORKDIR /app

# 4. OS Dependencies: Install LibPQ for PostgreSQL connection
RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 5. Cache Layer: Copy dependency definition files first
COPY pyproject.toml uv.lock ./

# 6. Install Libs: Sync dependencies from lock file
# --frozen: strict versioning | --no-install-project: libs only
RUN uv sync --frozen --no-dev --no-install-project

# 7. Environment: Auto-activate virtual environment
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# 8. Source Code: Copy pipeline module and main script
COPY docker_ingestion_pipeline/ ./docker_ingestion_pipeline/
COPY main.py .

# 9. Execution: Run the app
ENTRYPOINT ["python", "main.py"]