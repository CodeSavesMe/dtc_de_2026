# Module 01 — NYC Taxi Data Ingestion Pipeline

<div align="center">

![Docker](https://img.shields.io/badge/Docker-0db7ed?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)

</div>

This repository contains a Dockerized NYC TLC taxi ingestion pipeline built for **Module 01 (Docker & Terraform)** of the **Data Engineering Zoomcamp (DataTalksClub)**. It demonstrates an end-to-end, reproducible workflow: running PostgreSQL with Docker Compose, ingesting monthly taxi data via a CLI-driven pipeline (download → staging → validation → transactional promotion), and provisioning foundational GCP resources (GCS bucket + BigQuery dataset) with Terraform. The focus is on correctness, safe table promotion, and maintainable structure (Ports & Adapters), so the project can be reused as a reliable baseline for later modules.

---

## Contents
- [1. Project Overview](#1-project-overview)
- [2. Architecture & Design Principles](#2-architecture--design-principles)
- [3. Data Flow](#3-data-flow)
- [4. Project Structure](#4-project-structure)
- [5. Prerequisites](#5-prerequisites)
- [6. Quick Start](#6-quick-start)
- [7. Configuration](#7-configuration)
- [8. Running the Pipeline](#8-running-the-pipeline)
- [9. Command Cheat Sheet](#9-command-cheat-sheet)
- [10. Data Safety & Validation](#10-data-safety--validation)
- [11. Terraform Infrastructure (Module 01)](#11-terraform-infrastructure-module-01)
- [12. Notes & Known Limitations](#12-notes--known-limitations)
- [13. Learning Outcomes](#13-learning-outcomes)
- [14. Next Steps (Future Work)](#14-next-steps-future-work)


---

## 1. Project Overview

This project ingests NYC TLC taxi trip data into a PostgreSQL database using a custom Python ingestion pipeline.

The pipeline is:
- CLI-driven (explicit job execution)
- Containerized using Docker
- Designed with correctness and safety as first-class concerns

Terraform is included as part of the Module 01 requirements (GCS + BigQuery dataset provisioning).

---

## 2. Architecture & Design Principles

This project applies **Clean Architecture principles** using a **Ports & Adapters** structure.

The design emphasizes:
- Clear separation between orchestration, business logic, and infrastructure
- Database access abstracted behind interfaces (ports)
- Explicit ingestion lifecycle with staging and validation
- Safety mechanisms to prevent partial or corrupted loads

The goal is not full domain-driven design, but **clarity, extensibility, and correctness**.

---

## 3. Data Flow

For each ingestion job, the pipeline executes the following steps:

1. The pipeline is triggered via a CLI command (`ingest` or `ingest-url`)
2. The source file is downloaded locally
3. The file format is detected automatically (CSV / TSV / Parquet)
4. A PostgreSQL advisory lock is acquired for the target table
5. If the final table does not exist, it is created based on the source schema
6. A staging table is recreated from the final table schema (`<final>__staging`)
7. Data is loaded into the staging table using the appropriate loader
8. Validation checks are executed against the staging data
9. The staging table is promoted to the final table using a transactional swap
10. Post-load optimization: the SQL command `ANALYZE` is executed on the final table
11. The local file is optionally removed

---

## 4. Project Structure

```text
.
├── docker_ingestion_pipeline   # Core ingestion logic
│   ├── core                    # Pipeline orchestration
│   ├── db                      # Database implementations
│   ├── ports                   # Interfaces (ports)
│   └── utils                   # Shared utilities
├── data                        # Runtime download cache (default)
├── logs                        # Log files (app.log)
├── docker-compose.yml          # Local infrastructure
├── main.py                     # CLI entrypoint
├── terraform                   # GCP infrastructure (Module 01)
├── sql                         # Analytical SQL queries
└── notebooks                   # Exploratory analysis
```

---

## 5. Prerequisites

Before running this project, ensure you have:

- Docker & Docker Compose
- Python 3.10+ (optional for local runs; Docker is the recommended runtime)
- A Google Cloud account (for Terraform tasks)
- Terraform CLI (for provisioning GCP resources)

---

## 6. Quick Start

Run a complete ingestion job via Docker:

```bash
cp .env.example .env
docker compose up -d db
docker compose run --rm app ingest --taxi green --year 2021 --month 01
```

Taxi type for the standard command: `green` or `yellow`.

---

## 7. Configuration

This project is configured using environment variables and CLI flags.

### 7.1 Resolution order

1. System environment variables (Docker, CI, orchestrators)
2. Local `.env` file (loaded from the same directory as `main.py`, fallback only)

System variables always take precedence because `.env` is loaded with `override=False`.

### 7.2 Docker-specific behavior

* Variables are injected using Docker Compose `env_file`
* The `.env` file is **not mounted** into the container filesystem
* `DB_HOST` is overridden to point to the `db` service (via compose `environment`)

### 7.3 Loader settings

Loader performance tuning is configured via:

* `LOADER_CHUNK_SIZE` (default: `50000`)
* `LOADER_BATCH_SIZE` (default: `10000`)

### 7.4 Paths and logs

* `DATA_DIR` optionally overrides the runtime download directory (default: `./data`)
* Logs are written to `./logs/app.log`
* `ENABLE_PROGRESS` (default: true) enables tqdm-safe console output

---

## 8. Running the Pipeline

### 8.1 Service roles

* `db` runs as a long-lived PostgreSQL container
* `app` is a one-shot CLI-based ingestion job

Each ingestion job must be triggered explicitly using `docker compose run`.

### 8.2 Standard ingestion (DTC naming convention)

```bash
docker compose run --rm app ingest \
  --taxi green \
  --year 2021 \
  --month 01 \
  --file-format parquet \
  --if-exists replace
```

Notes:

* If not provided, `YEAR` defaults to `2025` (via CLI default).
* `--if-exists replace` performs staging + promotion.
* `--keep-local / --no-keep-local` controls whether the downloaded file is deleted after processing.

### 8.3 Custom ingestion from arbitrary URL

```bash
docker compose run --rm app ingest-url \
  --url "https://example.com/file.parquet" \
  --table-name "custom_table"
```

If `--table-name` is omitted, it will be inferred from the file name in the URL.

---

## 9. Command Cheat Sheet

> All commands below run the one-shot ingestion container (`app`).

### Standard ingestion (DTC URL + monthly table name)

```bash
docker compose run --rm app ingest --taxi green --year 2021 --month 01
```

### Choose file format (affects the download URL extension)

```bash
docker compose run --rm app ingest --taxi green --year 2021 --month 01 --file-format parquet
docker compose run --rm app ingest --taxi green --year 2021 --month 01 --file-format csv
```

### If table exists: choose strategy

```bash
# Replace (default): staging + transactional promotion (atomic swap)
docker compose run --rm app ingest --taxi green --year 2021 --month 01 --if-exists replace

# Skip: do nothing if table exists
docker compose run --rm app ingest --taxi green --year 2021 --month 01 --if-exists skip

# Fail: abort if table exists (exit code 2)
docker compose run --rm app ingest --taxi green --year 2021 --month 01 --if-exists fail

# Append: append data to existing table (behavior depends on loader implementation)
docker compose run --rm app ingest --taxi green --year 2021 --month 01 --if-exists append
```

### Keep or remove the downloaded file after ingestion

```bash
# Keep the downloaded file (default)
docker compose run --rm app ingest --taxi green --year 2021 --month 01 --keep-local

# Remove downloaded file after successful run
docker compose run --rm app ingest --taxi green --year 2021 --month 01 --no-keep-local
```

### Custom ingestion from an arbitrary URL

```bash
# Provide URL; table name is auto-inferred from filename if omitted
docker compose run --rm app ingest-url --url "https://example.com/file.parquet"

# Provide URL + explicit table name
docker compose run --rm app ingest-url \
  --url "https://example.com/file.parquet" \
  --table-name "custom_table"
```

### Run using environment variables (instead of flags)

```bash
# Example (local shell):
export TAXI=green YEAR=2021 MONTH=01 FILE_FORMAT=parquet IF_EXISTS=replace KEEP_LOCAL=true
python main.py ingest
```

Tips:

* Taxi type for `ingest`: `green` or `yellow`
* `ingest` builds the official DTC GitHub release URL automatically.
* `ingest-url` is for any URL (Parquet/CSV/TSV), optionally inferring `--table-name`.

---

## 10. Data Safety & Validation

### 10.1 Advisory lock (concurrency control)

The pipeline uses a PostgreSQL advisory lock per destination table:

* lock key format: `ingest:<final_table>`
* acquired using `pg_try_advisory_lock(hashtext(key)::bigint)`
* polling-based acquisition (default timeout: 60s, poll interval: 0.2s)
* session-scoped: the same database connection is held open while the lock is active

This prevents concurrent ingestion jobs from modifying the same table at the same time.

### 10.2 Staging tables

Data is always loaded into a staging table:

* staging name: `<final_table>__staging`
* staging is recreated on every run based on the final table schema

### 10.3 Validation behavior (PostgresStagingValidator)

Validation is executed after loading into staging:

* hard failure if the staging table has **0 rows**
* detects a datetime column if present (e.g. `lpep_pickup_datetime`, `tpep_pickup_datetime`, `pickup_datetime`)
* logs basic statistics (rowcount, min/max datetime, null count)
* if an expected month can be inferred from the destination table name (`YYYY_MM`), it checks month spillover:

  * absolute threshold: `> 200` rows outside the month
  * ratio threshold: `> 0.5%` rows outside the month
  * by default this produces a warning (does **not** fail the run)

### 10.4 Transactional promotion (AtomicSwapper)

Promotion from staging to final is performed inside a single transaction using rename operations:

* final is renamed to `<final>__backup` (if it exists)
* staging is renamed to `<final>`
* backup is dropped after a successful swap

### 10.5 Post-load optimization (PostLoadOptimizer)

After promotion, PostgreSQL statistics are updated using `ANALYZE` on the fully-qualified table (`schema.table`).

Cleanup operations (dropping staging tables during exception handling and deleting local files) are performed on a best-effort basis and do not affect data correctness.

---

## 11. Terraform Infrastructure (Module 01)

Terraform is included to satisfy the requirements of Module 01.

Provisioned resources:

* Google Cloud Storage (GCS) bucket
* BigQuery dataset

### Terraform quick commands

```bash
cd terraform
terraform init
terraform apply
```

> ### ⚠️ **Caution:**
> `terraform destroy` will permanently remove the resources created by this project.
> Run it after you're done testing — unless you enjoy surprise **invoices** in your inbox 💸

```bash
terraform destroy
```

This infrastructure is not integrated with the ingestion pipeline yet and serves as a foundation for later modules.

---

## 12. Notes & Known Limitations

This module focuses on local ingestion into PostgreSQL.

Out of scope for Module 01:

* workflow orchestration / scheduling
* streaming ingestion
* advanced database performance tuning (e.g., partitioning/indexing)

Terraform provisions a GCS bucket and a BigQuery dataset (Module 01 homework),
but the ingestion pipeline does not load data to GCS/BigQuery yet.

---

## 13. Learning Outcomes

By completing this module, the following concepts are practiced:

* Docker-based reproducible environments
* CLI-driven ingestion workflows
* Clean Architecture / Ports & Adapters in data pipelines
* Safe database ingestion patterns (staging + validation + transactional swap)
* Advisory locking with polling and timeout semantics
* Post-load PostgreSQL optimization using `ANALYZE`
* Infrastructure as Code using Terraform

## 14. Next Steps (Future Work)

A prioritized checklist of improvements and collaboration ideas (easy → hard).

### Quick wins
- [ ] Add GitHub Actions CI: lint + type checks + tests on every PR.
- [ ] Add `pre-commit` hooks to enforce formatting/linting locally.
- [ ] Add fail-fast config validation (clear errors for missing/invalid env vars).
- [ ] Add `run_id` / `job_id` and include it in every log line.
- [ ] Document idempotency and safe re-runs (especially `append` behavior).

### Reliability & test coverage
- [ ] Unit tests for deterministic helpers (URL builder, table naming, month inference).
- [ ] Integration tests with ephemeral Postgres (staging + swap + advisory lock).
- [ ] Add retries + exponential backoff for downloads and transient DB errors.
- [ ] Improve exit codes and error taxonomy (user vs infra vs data).

### Observability & ops hardening
- [ ] Optional structured logs (JSON) for log aggregation.
- [ ] Emit per-run metrics (duration, rowcount, outside-month ratio, bytes downloaded).
- [ ] Define alertable signals (0 rows, high spillover, abnormal runtime).
- [ ] Backup/restore notes + retention policy for tables and logs.
- [ ] Least privilege: separate DB roles (ingest vs read-only) and document grants.

### Longer-term / collaboration ideas
- [ ] Schema migrations/versioning (e.g., Alembic) for controlled evolution.
- [ ] Expand data quality rules (null policies, duplicates, ranges, outliers).
- [ ] Performance plan: benchmarks, optional indexes, partitioning guidance.
- [ ] Terraform hardening: remote state + locking, env separation, IAM notes, budget alerts 💸
- [ ] Orchestration (Kestra/Airflow): schedules, backfills, retries, alerting.
- [ ] Cloud loading path: local → GCS → BigQuery, then modeling (dbt).
- [ ] Run history table (e.g., `ingestion_runs`) for auditability.