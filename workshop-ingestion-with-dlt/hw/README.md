# Workshop — AI-Assisted Data Ingestion with dlt

Data Engineering Zoomcamp 2026 | From APIs to Warehouses with dlt + DuckDB

## Stack

- Python 3.12
- dlt 1.22.1
- DuckDB 1.4.4
- uv (package manager)

## Project Structure

```
workshop-ingestion-with-dlt/
├── .dlt/                          # dlt config and secrets
├── .github/                       # GitHub Actions
├── .venv/                         # Virtual environment (gitignored)
├── .vscode/
│   └── mcp.json                   # dlt MCP server config for Copilot
├── .gitignore
├── requirements.txt
├── taxi_pipeline_pipeline.py      # Main pipeline script
├── taxi_pipeline.duckdb           # Local DuckDB database (gitignored)
└── README.md
```

## Setup

### 1. Clone and navigate to the project

```bash
cd workshop-ingestion-with-dlt
```

### 2. Create and activate virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install "dlt[duckdb]" "dlt[workspace]"
```

### 4. Run the pipeline

```bash
python taxi_pipeline_pipeline.py
```

The pipeline will:
- Paginate through the NYC Taxi API (1,000 records per page)
- Stop automatically when an empty page is returned
- Load all data into a local DuckDB database

### 5. Inspect the data

```bash
dlt pipeline taxi_pipeline show
```

Opens the dlt dashboard at http://localhost:2718

## Data Source

| Property | Value |
|----------|-------|
| API | NYC Yellow Taxi Trip Data |
| Base URL | https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api |
| Format | Paginated JSON |
| Page size | 1,000 records |
| Pagination | Stop on empty page |
| Auth | None |

## Homework Results

Queries run against `taxi_pipeline_dataset.yellow_taxi_trips` in DuckDB:

```sql
-- Q1: Date range
SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time)
FROM taxi_pipeline_dataset.yellow_taxi_trips;
-- 2009-06-01 → 2009-07-01

-- Q2: Payment type breakdown
SELECT payment_type, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM taxi_pipeline_dataset.yellow_taxi_trips
GROUP BY payment_type ORDER BY pct DESC;
-- Credit: 26.66%

-- Q3: Total tips
SELECT ROUND(SUM(tip_amt), 2)
FROM taxi_pipeline_dataset.yellow_taxi_trips;
-- $6,063.41
```

| # | Question | Answer |
|---|----------|--------|
| 1 | Dataset date range | 2009-06-01 to 2009-07-01 |
| 2 | Proportion paid by credit card | 26.66% |
| 3 | Total tips generated | $6,063.41 |

## Key Concepts

**Why dlt?** dlt handles pagination, schema inference, type casting, and incremental loading automatically — replacing ~100 lines of boilerplate with ~20 lines of declarative config.

**Pagination fix:** The default `page_number` paginator expects a `total` key in the API response. Since this API returns a plain JSON array, `total_path: None` tells dlt to paginate until an empty page is returned.

**Column normalization:** dlt automatically converts camelCase/PascalCase to snake_case. `Trip_Pickup_DateTime` becomes `trip_pickup_date_time`.
