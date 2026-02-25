# Module 1 Homework: Workflow Orchestration

## NYC Taxi Data Pipeline - Airflow Orchestration

Workflow orchestration project for Module 2 using Apache Airflow to process NYC Taxi data.

## Architecture

```
GitHub CSV files → Airflow DAGs → Postgres Database
                    ↓
        Extract → Transform → Load → Validate
```

## Project Structure

```
02-airflow-orchestration/
├── dags/
│   ├── taxi_etl_yellow_dag.py      # Yellow taxi ETL pipeline
│   ├── taxi_etl_green_dag.py       # Green taxi ETL pipeline
│   ├── taxi_backfill_2020.py       # Backfill 2020 data (Q3, Q4)
│   └── taxi_backfill_2021.py       # Backfill 2021 data (Q5)
├── queries/
│   └── queries.sql                 # SQL queries for homework answers
├── tests/                          # Test files
├── docker-compose.yaml             # Infrastructure setup
├── .env.example                    # Environment variables template
└── README.md                       # This file
```

## Quick Start

### 1. Setup

```bash
# Clone and navigate to project
cd 02-airflow-orchestration

# Create environment file
cp .env.example .env

# Start infrastructure
docker compose up -d

# Wait ~2 minutes for initialization
docker compose ps  # Check all services are healthy
```

### 2. Access Airflow UI

Open http://localhost:8080
- Username: `admin`
- Password: `admin`

### 3. Run Pipelines

**For Homework Q1 (Yellow Dec 2020 file size):**
```bash
docker exec -it airflow-scheduler airflow dags trigger taxi_etl_yellow -e 2020-12-01
```

Check logs for file size in the extract task.

**For Homework Q3, Q4 (All 2020 data):**
```bash
# Enable Yellow and Green DAGs (toggle ON in UI)
# Enable backfill 2020 DAG
# Wait ~30-60 minutes for completion
```

**For Homework Q5 (2021 data):**
```bash
# Enable backfill 2021 DAG
# Wait ~20-40 minutes
```

### 4. Query Results

```bash
# Connect to Postgres
docker exec -it airflow-postgres psql -U airflow -d ny_taxi

# Run queries from queries/homework_queries.sql
\i /opt/airflow/queries/homework_queries.sql
```

## Homework Answers

### Q1: Yellow Dec 2020 File Size
Run `taxi_etl_yellow` for 2020-12-01, check extract task logs for file size.

### Q2: Variable Rendering
Answer: `green_tripdata_2020-04.csv`

### Q3: Yellow 2020 Total Rows
```sql
SELECT COUNT(*) FROM yellow_taxi_trips WHERE year=2020;
```

### Q4: Green 2020 Total Rows
```sql
SELECT COUNT(*) FROM green_taxi_trips WHERE year=2020;
```

### Q5: Yellow March 2021 Rows
```sql
SELECT COUNT(*) FROM yellow_taxi_trips WHERE year=2021 AND month=3;
```

### Q6: Timezone Configuration
In DAG definition:
```python
with DAG(
    'example_dag',
    schedule_interval='@daily',
    start_date=datetime(2020, 1, 1),
    timezone='America/New_York',  # ← This parameter
) as dag:
    pass
```

Answer: Add `timezone='America/New_York'` in the DAG configuration.

## Key Features

- **Idempotent pipelines**: Can be re-run without duplicates
- **Automated backfilling**: Catchup mechanism for historical data
- **Parallel execution**: Yellow and Green process simultaneously
- **Data quality checks**: Validation tasks verify data integrity
- **Production-ready**: Error handling, retries, logging

## Troubleshooting

### DAGs not appearing
- Wait 30 seconds for scheduler to parse DAGs
- Check `docker compose logs airflow-scheduler` for import errors

### Task failures
- Click failed task in UI → View logs
- Common issues: CSV file doesn't exist for that date (use 2020-2021 dates only)

### Database connection issues
- Verify Postgres is healthy: `docker compose ps`
- Check .env file has correct credentials

## Cleanup

```bash
# Stop services
docker compose down

# Remove all data (including database)
docker compose down -v
```

## Technologies

- **Apache Airflow 2.10.0**: Workflow orchestration
- **PostgreSQL 16**: Data warehouse
- **Python 3.11**: DAG development
- **Docker Compose**: Infrastructure management
- **Pandas**: Data transformation