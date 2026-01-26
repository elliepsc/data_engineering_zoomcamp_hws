#!/bin/bash
# =============================================================================
# Postgres Initialization - Create both required databases
# =============================================================================
# Postgres creates only one DB by default (POSTGRES_DB)
# But Airflow needs a separate airflow_metadata database
# This script runs automatically on first startup
# =============================================================================

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airflow_metadata'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_metadata')\gexec
EOSQL

echo "✅ Databases initialized: ny_taxi, airflow_metadata"