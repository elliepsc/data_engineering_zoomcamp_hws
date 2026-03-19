# NYC Taxi Analytics - dbt Project

## Overview

This dbt project transforms raw NYC taxi data into analytics-ready models.

## Project Structure

```
taxi_analytics/
├── models/
│   ├── schema.yml              # Source definitions
│   ├── staging/
│   │   └── stg_green_taxi.sql  # Staging: cleaned raw data
│   └── core/
│       ├── fact_trips.sql      # Fact table: trip details
│       └── monthly_revenue.sql # Aggregate: monthly metrics
├── tests/
│   └── assert_positive_fares.sql
├── dbt_project.yml
├── profiles.yml
└── packages.yml
```

## Models

### Staging Layer

**stg_green_taxi**
- Cleans raw data
- Standardizes data types
- Adds calculated fields (trip_duration, is_suspicious)
- Filters invalid records

### Core Layer

**fact_trips**
- Analytics-ready trip fact table
- Partitioned by pickup_datetime (monthly)
- Clustered by location IDs
- Includes date dimensions and calculated metrics

**monthly_revenue**
- Aggregated monthly metrics
- Revenue, distance, and passenger counts
- Month-over-month growth calculations

## Running dbt

### From Command Line

```bash
# Install dependencies
dbt deps --target dev

# Run all models
dbt run --target dev

# Run tests
dbt test --target dev

# Generate documentation
dbt docs generate --target dev
dbt docs serve --port 8001
```

### From Makefile

```bash
# Install dependencies
make dbt-deps

# Run models
make dbt-run

# Run tests
make dbt-test

# View documentation
make dbt-docs
```

### From Airflow

Models are automatically run by the `02_transform_dbt_models` DAG after data ingestion completes.

## Tests

- **Source tests**: Not null checks on pickup/dropoff timestamps
- **Custom tests**: Positive fare amounts
- **dbt tests**: Data quality validations

## Configuration

Edit `profiles.yml` to configure BigQuery connection:
- **dev target**: For local development
- **prod target**: For Airflow orchestration

## Dependencies

- `dbt-bigquery`: BigQuery adapter
- `dbt-utils`: Utility macros (surrogate_key, etc.)
