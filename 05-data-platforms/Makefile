# ============================================
# Makefile - Airflow Data Platform
# ============================================

.PHONY: help check-env setup-gcloud-permissions start stop restart clean clean-all logs status shell-webserver shell-scheduler test

# Default target
.DEFAULT_GOAL := help

# ============================================
# Help
# ============================================
help:
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Airflow Data Platform - Available Commands"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "  🚀 Core Commands:"
	@echo "    make start              Start Airflow (with gcloud setup)"
	@echo "    make stop               Stop Airflow"
	@echo "    make restart            Restart Airflow"
	@echo "    make logs               Show logs (all services)"
	@echo "    make status             Show container status"
	@echo ""
	@echo "  🔧 Setup Commands:"
	@echo "    make setup-gcloud       Setup gcloud permissions"
	@echo "    make venv               Create Python virtual environment"
	@echo ""
	@echo "  🗄️  BigQuery Commands:"
	@echo "    make reset-tables       Drop all BigQuery tables"
	@echo "    make reset-green        Drop green_taxi table only"
	@echo "    make restart-pipeline   Drop tables + restart DAG backfill"
	@echo ""
	@echo "  🧹 Cleanup Commands:"
	@echo "    make clean              Stop and remove containers"
	@echo "    make clean-all          Clean everything (including volumes)"
	@echo ""
	@echo "  🐚 Debug Commands:"
	@echo "    make shell-webserver    Shell into webserver container"
	@echo "    make shell-scheduler    Shell into scheduler container"
	@echo "    make test               Run tests"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"


# ============================================
# Environment Check
# ============================================
check-env:
	@if [ ! -f .env ]; then \
		echo "❌ .env file not found!"; \
		echo "   Copy .env.example to .env and configure it"; \
		exit 1; \
	fi
	@echo "✓ .env file configured"

# ============================================
# BigQuery Management
# ============================================
# Load environment variables
include .env
export

reset-green:
	@echo "🗑️  Dropping green_taxi table..."
	@bq rm -f -t $(GCP_PROJECT_ID):raw_data.green_taxi || true
	@echo "✅ green_taxi table dropped!"

reset-yellow:
	@echo "🗑️  Dropping yellow_taxi table..."
	@bq rm -f -t $(GCP_PROJECT_ID):raw_data.yellow_taxi || true
	@echo "✅ yellow_taxi table dropped!"

reset-tables: reset-green reset-yellow
	@echo "✅ All taxi tables dropped!"

restart-pipeline: reset-tables
	@echo ""
	@echo "🔄 Restarting pipeline from scratch..."
	@echo "   This will clear all DAG runs and restart the backfill"
	@echo ""
	@read -p "Continue? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker exec -it airflow-scheduler airflow dags backfill \
			01_ingest_nyc_taxi_incremental \
			--start-date $(DATA_START_DATE) \
			--end-date $(DATA_END_DATE) \
			--reset-dagruns \
			--yes; \
		echo ""; \
		echo "✅ Pipeline restart initiated!"; \
		echo "   Check Airflow UI to monitor progress"; \
	else \
		echo "Cancelled"; \
	fi


# ============================================
# GCloud Setup (NEW!)
# ============================================
setup-gcloud-permissions:
	@echo "Setting up gcloud credentials permissions..."
	@if [ -f ~/.config/gcloud/application_default_credentials.json ]; then \
		chmod 644 ~/.config/gcloud/application_default_credentials.json; \
		echo "✓ Permissions set to 644 (readable by Docker)"; \
	else \
		echo "⚠️  Credentials file not found!"; \
		echo ""; \
		echo "Run these commands first:"; \
		echo "  1. gcloud auth login"; \
		echo "  2. gcloud config set project YOUR-PROJECT-ID"; \
		echo "  3. gcloud auth application-default login"; \
		echo ""; \
		exit 1; \
	fi

# Alias for easier use
setup-gcloud: setup-gcloud-permissions

# ============================================
# Start Airflow (with automatic gcloud setup)
# ============================================
start: check-env setup-gcloud-permissions
	@echo "Starting Airflow..."
	docker compose up -d
	@echo "✓ Airflow started!"
	@echo "  UI: http://localhost:8082"
	@echo "  Username: admin"
	@echo "  Password: admin (or check .env)"
	@echo ""
	@echo "Run 'make logs' to see logs"
	@echo "Run 'make status' to check health"

# ============================================
# Stop Airflow
# ============================================
stop:
	@echo "Stopping Airflow..."
	docker compose stop
	@echo "✓ Airflow stopped"

# ============================================
# Restart Airflow
# ============================================
restart: setup-gcloud-permissions
	@echo "Restarting Airflow..."
	docker compose restart
	@echo "✓ Airflow restarted"

# ============================================
# Clean (stop and remove containers)
# ============================================
clean:
	@echo "Cleaning up containers..."
	docker compose down
	@echo "✓ Cleanup complete"

# ============================================
# Clean All (including volumes)
# ============================================
clean-all:
	@echo "⚠️  This will delete all containers, networks, and volumes!"
	@echo "   Database data will be lost!"
	@read -p "Continue? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker compose down -v; \
		echo "✓ Complete cleanup done"; \
	else \
		echo "Cancelled"; \
	fi

# ============================================
# Logs
# ============================================
logs:
	docker compose logs -f

# Show logs for specific service
logs-webserver:
	docker compose logs -f airflow-webserver

logs-scheduler:
	docker compose logs -f airflow-scheduler

logs-postgres:
	docker compose logs -f postgres

# ============================================
# Status
# ============================================
status:
	@echo "Container Status:"
	@docker compose ps

# ============================================
# Shell Access
# ============================================
shell-webserver:
	docker exec -it airflow-webserver bash

shell-scheduler:
	docker exec -it airflow-scheduler bash

shell-postgres:
	docker exec -it airflow-postgres psql -U airflow -d airflow

# ============================================
# Virtual Environment Setup
# ============================================
venv:
	@echo "Creating Python virtual environment..."
	python3 -m venv .venv
	@echo "✓ Virtual environment created"
	@echo ""
	@echo "Activate it with:"
	@echo "  source .venv/bin/activate"
	@echo ""
	@echo "Then install dependencies:"
	@echo "  pip install -r requirements-dev.txt"

# ============================================
# Testing
# ============================================
test:
	@echo "Running tests..."
	pytest tests/ -v

# ============================================
# Development Utilities
# ============================================
# Format Python code
format:
	black airflow/ dbt/ tests/

# Lint Python code
lint:
	flake8 airflow/ dbt/ tests/

# Check if DAGs have syntax errors
validate-dags:
	python -m airflow.utils.dag_processing airflow/dags/

