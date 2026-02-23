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
	@echo "  make clean       - Remove containers and volumes"
	@echo "  make logs        - Show container logs"
	@echo ""
	@echo "📊 Data Commands:"
	@echo "  make data        - Download NYC taxi data (60 parquet files)"
	@echo "  make setup-dbt   - Install dbt deps + download seed file (if missing)"
	@echo "  make download-seed - Force download seed file (overwrites existing)"
	@echo "  make clean-data  - Remove downloaded data"
	@echo ""
	@echo "🔧 dbt Commands:"
	@echo "  make dbt-debug   - Test dbt connection"
	@echo "  make dbt-deps    - Install dbt packages"
	@echo "  make dbt-seed    - Load seed files (taxi_zone_lookup)"
	@echo "  make dbt-run     - Run all models"
	@echo "  make dbt-test    - Run all tests"
	@echo "  make dbt-build   - Run seed + run + test (all-in-one)"
	@echo "  make dbt-docs    - Generate and serve documentation"
	@echo "  make dbt-clean   - Remove dbt artifacts"
	@echo ""
	@echo "📝 Homework Commands:"
	@echo "  make homework-2026      - Run base homework queries (Q1-Q6)"
	@echo "  make homework-2025      - Run advanced analytics (2025 level)"
	@echo "  make homework-all       - Run both"
	@echo "  make verify-models      - Check all models are built"
	@echo ""
	@echo "🧹 Cleanup Commands:"
	@echo "  make dbt-clean          - Remove dbt artifacts only"
	@echo "  make clean              - Remove container + volumes"
	@echo "  make clean-data         - Remove downloaded data"
	@echo "  make clean-all          - Remove data + artifacts (keeps Docker image)"
	@echo "  make reset              - Reset containers (keeps data & image)"
	@echo "  make reset-all          - FULL reset (removes everything inc. data)"
	@echo ""
	@echo "🛠️  Utility Commands:"
	@echo "  make install-duckdb-cli - Install DuckDB CLI in container"
	@echo ""
	@echo "🚀 Quick Start:"
	@echo "  make build && make up && make data && make setup-dbt && make dbt-build"

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
# 	python download_data.py
	docker-compose exec dbt python3 /workspace/download_data.py


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
	@echo "⚠️  dbt will say localhost:8080 - IGNORE THIS"
	@echo "!!!!!!! ✅ Use http://localhost:38080 instead !!!!!!"
	@echo "   Press Ctrl+C to stop"
	docker-compose exec dbt dbt docs serve --port 8080

# Combined workflows
setup: build up data
	@echo "✅ Setup complete! Ready to run dbt commands."

setup-dbt:
	@echo "🔧 Setting up dbt..."
	docker-compose exec dbt dbt deps
	@echo "📥 Checking taxi zone lookup seed file..."
	@docker-compose exec dbt bash -c '\
		if [ -f /workspace/taxi_rides_ny/seeds/taxi_zone_lookup.csv ]; then \
			echo "✅ Seed file already exists, skipping download"; \
		else \
			echo "📥 Downloading taxi_zone_lookup.csv..."; \
			cd /workspace/taxi_rides_ny/seeds && \
			wget -q -O taxi_zone_lookup.csv https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv && \
			echo "✅ Seed file downloaded"; \
		fi'
	@echo "✅ dbt setup complete!"

download-seed:
	@echo "📥 Downloading taxi zone lookup seed file (forced)..."
	docker-compose exec dbt bash -c "cd /workspace/taxi_rides_ny/seeds && wget -q -O taxi_zone_lookup.csv https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
	@echo "✅ Seed file downloaded!"

full-build: dbt-deps dbt-seed dbt-run dbt-test
	@echo "✅ Full dbt build complete!"

# Homework queries
homework-2026:
	@echo "📝 Running homework 2026 queries..."
	docker-compose exec dbt python3 /workspace/queries/homework_queries_2026.py

homework-2026-table:
	@echo "📊 Running homework 2026 queries (with tables)..."
	docker-compose exec dbt python3 /workspace/queries/homework_queries_2026_tabulate.py

homework-2025:
	@echo "🎯 Running advanced analytics (2025 level)..."
	docker-compose exec dbt python3 /workspace/queries/homework_queries_2025.py

homework-all: homework-2026 homework-2025

verify-models:
	@echo "✅ Verifying all models built..."
	@docker-compose exec -T dbt python3 << 'EOF'
	import duckdb
	conn = duckdb.connect('/workspace/taxi_rides_ny/taxi_rides_ny.duckdb')
	models = ['stg_green_tripdata', 'stg_yellow_tripdata', 'stg_fhv_tripdata',
	          'int_trips_unioned', 'fct_trips', 'dim_zones',
	          'fct_monthly_zone_revenue', 'fct_monthly_fare_percentiles',
	          'fct_monthly_revenue_growth']
	for model in models:
	    try:
	        result = conn.execute(f"SELECT COUNT(*) FROM {model}").fetchone()
	        print(f"✅ {model:30} {result[0]:,} records")
	    except Exception as e:
	        print(f"❌ {model:30} ERROR")
	conn.close()
	EOF

# Cleaning
dbt-clean:
	@echo "🧹 Cleaning dbt artifacts..."
	docker-compose exec dbt bash -c "cd /workspace/taxi_rides_ny && rm -rf target/ dbt_packages/ logs/ *.duckdb*"
	@echo "✅ dbt artifacts cleaned!"

clean-data:
	@echo "🧹 Removing downloaded data..."
	rm -rf data/
	@echo "✅ Data removed!"

clean-all: dbt-clean clean-data
	@echo "🧹 Full cleanup (data + artifacts + seeds)..."
	docker-compose exec dbt bash -c "rm -rf /workspace/taxi_rides_ny/seeds/*_README.md" 2>/dev/null || true
	@echo "✅ Everything cleaned!"

reset:
	@echo "🔄 Reset (containers + volumes, keeps data & image)"
	@echo "⚠️  Data files will be preserved"
	docker-compose down -v
	@echo ""
	@echo "✅ Reset complete!"
	@echo ""
	@echo "Data kept in data/ directory"
	@echo "To restart: make up && make dbt-build"

reset-all: clean-all
	@echo "🔄 FULL RESET (containers + image + data + everything)"
	@echo "⚠️  This will DELETE all data files (~3GB)"
	@echo ""
	docker-compose down -v
	docker rmi 04-analytics-engineering-dbt 2>/dev/null || true
	@echo ""
	@echo "✅ Full reset complete!"
	@echo ""
	@echo "To rebuild from scratch:"
	@echo "  make build && make up && make data && make setup-dbt && make dbt-build"

install-duckdb-cli:
# 	@echo "📦 Installing DuckDB CLI..."
# 	docker-compose exec dbt bash -c "cd /tmp && wget -q https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip && unzip -q duckdb_cli-linux-amd64.zip && mv duckdb /usr/local/bin/ && rm duckdb_cli-linux-amd64.zip"
# 	@echo "✅ DuckDB CLI installed! Run: make shell && duckdb taxi_rides_ny.duckdb"
install-duckdb-cli:
	@echo "📦 Installing DuckDB CLI..."
	@docker-compose exec dbt bash -c '\
		apt-get update -qq && \
		apt-get install -y -qq unzip && \
		cd /tmp && \
		wget -q https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip && \
		unzip -q duckdb_cli-linux-amd64.zip && \
		mv duckdb /usr/local/bin/ && \
		rm duckdb_cli-linux-amd64.zip && \
		echo "✅ DuckDB CLI installed!"'
	@echo ""
	@echo "To use: docker-compose exec dbt bash"
	@echo "        duckdb taxi_rides_ny.duckdb"

# Alternative: run dbt commands locally (without Docker)
local-run:
	cd taxi_rides_ny && dbt run

local-test:
	cd taxi_rides_ny && dbt test

local-build:
	cd taxi_rides_ny && dbt build
