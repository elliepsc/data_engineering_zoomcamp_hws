# Makefile for Module 4 - Analytics Engineering
# Simplifies Docker and dbt commands

.PHONY: help build up down shell clean data dbt-debug dbt-deps dbt-seed dbt-run dbt-test dbt-build dbt-docs

# Default target
help:
	@echo "📚 Module 4 - Analytics Engineering Commands"
	@echo ""
	@echo "🐳 Docker Commands:"
	@echo "  make build       - Build Docker image"
	@echo "  make up          - Start container in background"
	@echo "  make down        - Stop and remove container"
	@echo "  make shell       - Open bash shell in container"
	@echo "  make clean       - Remove all containers and volumes"
	@echo ""
	@echo "📊 Data Commands:"
	@echo "  make data        - Download NYC taxi data (60 parquet files)"
	@echo ""
	@echo "🔧 dbt Commands:"
	@echo "  make dbt-debug   - Test dbt connection"
	@echo "  make dbt-deps    - Install dbt packages"
	@echo "  make dbt-seed    - Load seed files (taxi_zone_lookup)"
	@echo "  make dbt-run     - Run all models"
	@echo "  make dbt-test    - Run all tests"
	@echo "  make dbt-build   - Run seed + run + test"
	@echo "  make dbt-docs    - Generate and serve documentation"
	@echo ""
	@echo "🚀 Quick Start:"
	@echo "  make build && make up && make data && make dbt-build"

# Docker commands
build:
	@echo "🐳 Building Docker image..."
	docker-compose build

up:
	@echo "🚀 Starting container..."
	docker-compose up -d

down:
	@echo "🛑 Stopping container..."
	docker-compose down

shell:
	@echo "💻 Opening shell in container..."
	docker-compose exec dbt bash

clean:
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	rm -f taxi_rides_ny/taxi_rides_ny.duckdb*
	rm -rf taxi_rides_ny/target/
	rm -rf taxi_rides_ny/dbt_packages/

# Data commands
data:
	@echo "📊 Downloading NYC taxi data..."
	docker-compose exec dbt python /workspace/download_data.py  
	# previous: python download_data.py but run locally

# dbt commands (run inside container)
dbt-debug:
	@echo "🔍 Testing dbt connection..."
	docker-compose exec dbt dbt debug

dbt-deps:
	@echo "📦 Installing dbt packages..."
	docker-compose exec dbt dbt deps

dbt-seed:
	@echo "🌱 Loading seed files..."
	docker-compose exec dbt dbt seed

dbt-run:
	@echo "🏃 Running dbt models..."
	docker-compose exec dbt dbt run

dbt-test:
	@echo "🧪 Running dbt tests..."
	docker-compose exec dbt dbt test

dbt-build:
	@echo "🏗️  Building everything (seed + run + test)..."
	docker-compose exec dbt dbt build

dbt-docs:
	@echo "📚 Generating and serving dbt docs..."
	docker-compose exec dbt dbt docs generate
	@echo "🌐 Starting dbt docs server on http://localhost:38080"
	@echo "   (Container port 8080 → Host port 38080)"
	@echo "   Press Ctrl+C to stop"
	docker-compose exec dbt dbt docs serve --port 8080

# Combined workflows
setup: build up data
	@echo "✅ Setup complete! Ready to run dbt commands."

full-build: dbt-deps dbt-seed dbt-run dbt-test
	@echo "✅ Full dbt build complete!"

# Alternative: run dbt commands locally (without Docker)
local-run:
	cd taxi_rides_ny && dbt run

local-test:
	cd taxi_rides_ny && dbt test

local-build:
	cd taxi_rides_ny && dbt build
