# Module 01 - Docker & Terraform (DTC DE Zoomcamp 2026)
# - Matches current CLI: `python main.py ingest` and `python main.py ingest-url`
# - Matches docker-compose services: `db` and `app`

SHELL := /bin/bash

# --- Load .env (optional) ---
ifneq ("$(wildcard .env)","")
	include .env
	export $(shell sed 's/=.*//' .env)
endif

# --- Tools ---
COMPOSE ?= docker compose
PYTHON  ?= python
TF      ?= terraform

# --- Paths ---
TF_DIR  ?= terraform

# --- Defaults (override in command line) ---
# Example:
#   make ingest TAXI=green YEAR=2021 MONTH=01 FILE_FORMAT=csv IF_EXISTS=replace KEEP_LOCAL=false
TAXI        ?= green
YEAR        ?= 2021
MONTH       ?= 01
FILE_FORMAT ?= parquet
IF_EXISTS   ?= replace
KEEP_LOCAL  ?= true

# For ingest-url
DATA_URL    ?=
TABLE_NAME  ?=

.DEFAULT_GOAL := help

.PHONY: help env up-db up down reset logs psql \
        ingest ingest-url \
        tf-init tf-apply tf-destroy \
        clean clean-data

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

env: ## Create .env from .env.example if missing
	@if [ ! -f .env ]; then \
		if [ -f .env.example ]; then cp .env.example .env && echo "Created .env from .env.example"; \
		else echo "ERROR: .env.example not found"; exit 1; fi \
	else echo ".env already exists"; fi

# -------------------------
# Docker
# -------------------------
up-db: env ## Start Postgres only (recommended)
	$(COMPOSE) up -d db

up: env ## Start db + app (app is a runnable container; use `make ingest` for jobs)
	$(COMPOSE) up -d db

down: ## Stop and remove containers
	$(COMPOSE) down

reset: ## Reset containers (keeps ny_data folder unless you run clean-data)
	$(COMPOSE) down
	$(COMPOSE) up -d db
	@echo "Services restarted."

logs: ## Tail app logs (container output)
	$(COMPOSE) logs -f app

psql: ## Open psql inside db container
	$(COMPOSE) exec db psql -U $${DB_USER:-postgres} -d $${DB_NAME:-ny_taxi}

# -------------------------
# Ingestion (Docker - recommended)
# -------------------------
ingest: up-db ## Run standard ingestion (DTC URL). Override vars: TAXI/YEAR/MONTH/FILE_FORMAT/IF_EXISTS/KEEP_LOCAL
	@KEEP_FLAG="--keep-local"; \
	if [ "$(KEEP_LOCAL)" = "false" ] || [ "$(KEEP_LOCAL)" = "0" ]; then KEEP_FLAG="--no-keep-local"; fi; \
	$(COMPOSE) run --rm app ingest \
		--taxi $(TAXI) \
		--year $(YEAR) \
		--month $(MONTH) \
		--file-format $(FILE_FORMAT) \
		--if-exists $(IF_EXISTS) \
		$$KEEP_FLAG

ingest-url: up-db ## Run ingestion from custom URL (DATA_URL required; TABLE_NAME optional)
	@if [ -z "$(DATA_URL)" ]; then \
		echo "ERROR: DATA_URL is required."; \
		echo "Example: make ingest-url DATA_URL='https://example.com/file.parquet' TABLE_NAME=my_table"; \
		exit 1; \
	fi
	@KEEP_FLAG="--keep-local"; \
	if [ "$(KEEP_LOCAL)" = "false" ] || [ "$(KEEP_LOCAL)" = "0" ]; then KEEP_FLAG="--no-keep-local"; fi; \
	if [ -z "$(TABLE_NAME)" ]; then \
		$(COMPOSE) run --rm app ingest-url --url "$(DATA_URL)" $$KEEP_FLAG; \
	else \
		$(COMPOSE) run --rm app ingest-url --url "$(DATA_URL)" --table-name "$(TABLE_NAME)" $$KEEP_FLAG; \
	fi

# -------------------------
# Terraform (Module 01)
# -------------------------
tf-init: ## terraform init
	cd $(TF_DIR) && $(TF) init

tf-apply: ## terraform apply
	cd $(TF_DIR) && $(TF) apply

tf-destroy: ## terraform destroy (careful 💸)
	@echo "CAUTION: This will delete Terraform-managed resources."
	@echo "Run it when you're done testing (unless you enjoy surprise invoices 💸)."
	cd $(TF_DIR) && $(TF) destroy

# -------------------------
# Cleanup
# -------------------------
clean: ## Remove python caches and local logs (safe)
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	rm -f logs/*.log || true
	@echo "Clean done."

clean-data: ## DANGER: stop containers and delete Postgres data folder (01-docker-terraform/ny_data)
	@echo "DANGER: this will delete local Postgres data under ./ny_data"
	@echo "Stopping containers..."
	$(COMPOSE) down
	@read -p "Type 'DELETE' to continue: " ans; \
	if [ "$$ans" = "DELETE" ]; then rm -rf ny_data && echo "ny_data removed."; else echo "Canceled."; fi
