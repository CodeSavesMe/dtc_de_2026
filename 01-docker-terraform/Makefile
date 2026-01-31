# Makefile

#------- Configuration -------
SHELL := /bin/bash

# Load environment variables from .env if the file exists
ifneq ("$(wildcard .env)","")
    include .env
    export $(shell sed 's/=.*//' .env)
endif

#------- Paths & Variables -------
# Python command (change if using an alternative runner)
PYTHON := python
INGEST_SCRIPT := main.py

# Declare phony targets to avoid conflicts with files
.PHONY: help up down reset ingest clean clean-data

#------- Help -------
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

#------- Docker Services -------
up: ## Start database services (Postgres + pgAdmin)
	docker-compose up -d

down: ## Stop database services
	docker-compose down

reset: ## Stop services, remove volumes, and start fresh
	docker-compose down -v
	docker-compose up -d
	@echo "Docker services have been reset."

#------- Data Ingestion -------
ingest: ## Run data ingestion. Usage: make ingest TABLE=mytable URL=http://...
	@if [ -z "$(TABLE)" ]; then echo "Error: TABLE is required. Example: make ingest TABLE=green_tripdata_2019_10"; exit 1; fi
	@# Run ingestion script from project root
	$(PYTHON) $(INGEST_SCRIPT) --table_name=$(TABLE) $(if $(URL),--url=$(URL),)

#------- Cleanup -------
clean: ## Remove Python cache, log files, and temporary files
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -f logs/*.log
	# Optional: remove raw data files if desired
	# rm -f data/*.csv data/*.parquet
	@echo "Clean up finished."

#------- Database Data Cleanup -------
clean-data: ## DANGER: Stop containers and delete 'ny_data' folder (requires sudo)
	@echo "Stopping containers..."
	docker-compose down
	@echo "Removing Postgres data volume (sudo permissions required)..."
	sudo rm -rf ny_data
	@echo "Database data wiped. You can now run 'make up' for a fresh DB."
