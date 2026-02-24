# Airflow Data Platform - NYC Taxi Analytics

Warning: I do not follow instructions from the zoomcamp.: I do not use Bruin.
I work on data platform mixing ETL(dbt) & airflow

> **Production-ready data platform demonstrating modern data engineering patterns with Apache Airflow, dbt, and BigQuery.**

Built for the [DataTalks.Club Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp) - Module 5: Data Platforms

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Airflow 2.8.1](https://img.shields.io/badge/airflow-2.8.1-orange.svg)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-green.svg)](https://www.getdbt.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Table of Contents

- [Homework Answers](#homework-answers) 📝
- [Architecture Overview](#architecture-overview)
- [Why This Architecture?](#why-this-architecture)
- [Production-Ready Patterns](#production-ready-patterns) ⭐
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Pipeline Details](#pipeline-details)
- [Key Learnings](#key-learnings)
- [Next Steps](#next-steps)

---

## 📝 Homework Answers

**Module 5 was designed for Bruin**, but this project implements the **same data platform concepts** using **Airflow + dbt + BigQuery**.

**See [HOMEWORK_ANSWERS.md](HOMEWORK_ANSWERS.md)** for detailed mapping of:
- Question 1: Pipeline Structure → Airflow project structure
- Question 2: Materialization → Incremental loading with BigQuery partitions
- Question 3: Variables → Environment variables & Airflow Variables
- Question 4: Dependencies → ExternalTaskSensor & DAG triggers
- Question 5: Quality Checks → DataQualityOperator & dbt tests
- Question 6: Lineage → Airflow Graph View & dbt docs
- Question 7: First-Time Run → CREATE_IF_NEEDED disposition

**Why Airflow instead of Bruin?**
- 70% of data engineering jobs require Airflow (<1% mention Bruin)
- Production-proven at Airbnb, Reddit, Lyft, etc.
- Better for portfolio and employability

---

## 🏗️ Architecture Overview

This data platform orchestrates a complete **Extract-Load-Transform (ELT)** pipeline for NYC Taxi data:

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCE                                │
│  NYC TLC Taxi Trip Records (Parquet files)                      │
│  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│               AIRFLOW ORCHESTRATION LAYER                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DAG 1: Incremental Data Ingestion                       │  │
│  │  ├── Download monthly Parquet files (2019-2020)          │  │
│  │  ├── Upload to Google Cloud Storage                      │  │
│  │  └── Load to BigQuery (partitioned + clustered)          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                         │                                       │
│                         ▼                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DAG 2: dbt Transformations                              │  │
│  │  ├── Wait for ingestion (ExternalTaskSensor)             │  │
│  │  ├── Run staging models (cleaning, deduplication)        │  │
│  │  ├── Run core models (business logic, aggregations)      │  │
│  │  └── Execute dbt tests                                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                         │                                       │
│                         ▼                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DAG 3: Data Quality Monitoring                          │  │
│  │  ├── Check data freshness                                │  │
│  │  ├── Validate NULL constraints                           │  │
│  │  ├── Verify value ranges (trip distance, fares)          │  │
│  │  └── Monitor anomalies                                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE                                │
│  Google BigQuery                                                │
│  ├── raw_data: Raw ingested data                               │
│  ├── staging: Cleaned and standardized                         │
│  └── analytics: Business-ready aggregations                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Why This Architecture?

### Airflow as Orchestrator

- **Industry Standard**: 70%+ of data engineering job postings require Airflow
- **Python-First**: Flexible, testable, and familiar to data engineers
- **Rich Ecosystem**: 500+ pre-built operators and integrations
- **Production-Proven**: Used by Airbnb, Reddit, Lyft, and thousands of companies

### BigQuery as Data Warehouse

- **Serverless**: No infrastructure management
- **Performance**: Petabyte-scale analytics with columnar storage
- **Cost-Effective**: Pay only for queries and storage used
- **Integration**: Native support in Airflow and dbt

### dbt for Transformations

- **SQL-Based**: Familiar to analysts and data engineers
- **Version Control**: Models as code with Git
- **Testing**: Built-in data quality framework
- **Documentation**: Auto-generated lineage and docs

---

## ⭐ Production-Ready Patterns

This project goes beyond "homework code" to demonstrate **professional data engineering practices** that you'll use in production environments.

### 1. Custom Operators (`plugins/`)

**Problem**: Repeating the same logic across multiple DAGs leads to code duplication and maintenance nightmares.

**Solution**: Create reusable Airflow operators that encapsulate common patterns.

#### Example: DataQualityOperator

Instead of writing 50 lines of BigQuery validation code in each DAG:

```python
# ❌ BAD: Repeated in every DAG
def check_data_quality(**context):
    from google.cloud import bigquery
    client = bigquery.Client()
    query = "SELECT COUNT(*) FROM table WHERE column IS NULL"
    result = client.query(query).result()
    # ... 40 more lines of validation logic ...
```

Use a custom operator once:

```python
# ✅ GOOD: Reusable across all DAGs
from operators.data_quality_operator import DataQualityOperator

check_nulls = DataQualityOperator(
    task_id='check_no_nulls',
    table_id='raw_data.green_taxi',
    check_type='null_check',
    column='lpep_pickup_datetime',
)
```

**Benefits**:
- **DRY Principle**: Write once, use everywhere
- **Testability**: Unit test the operator independently
- **Maintainability**: Fix bugs in one place
- **Professionalism**: Shows you understand software engineering

**Location**: `airflow/plugins/operators/data_quality_operator.py`

---

### 2. Centralized Configuration (`config/`)

**Problem**: Hardcoded values scattered across DAGs make it impossible to switch environments or update credentials.

**Solution**: Single source of truth for all configuration via environment variables and config loaders.

#### Example: Config Loader

Instead of hardcoding project IDs in every DAG:

```python
# ❌ BAD: Hardcoded everywhere
PROJECT_ID = "my-project-123"
DATASET = "raw_data"
BUCKET = "gs://my-bucket"
```

Load from centralized config:

```python
# ✅ GOOD: Load from environment
from utils.config_loader import get_gcp_config, get_bigquery_config

gcp_config = get_gcp_config()
bq_config = get_bigquery_config()

PROJECT_ID = gcp_config['project_id']
DATASET = bq_config['dataset_raw']
```

**Benefits**:
- **Environment Switching**: Change `.env` file to switch dev/staging/prod
- **Security**: No credentials in code (Git safe)
- **Maintainability**: Update config in one place
- **12-Factor App**: Follows industry best practices

**Location**: `airflow/utils/config_loader.py`

---

### 3. Reusable Utilities (`utils/`)

**Problem**: Common operations (GCS uploads, BigQuery queries, logging) are reimplemented in every DAG.

**Solution**: Helper functions with error handling, retries, and structured logging.

#### Example: Structured Logging

Instead of basic print statements:

```python
# ❌ BAD: Unparseable logs
print("Starting upload")
print(f"File size: {size}")
```

Use structured JSON logging:

```python
# ✅ GOOD: Machine-parseable logs
from utils.logger import get_logger

logger = get_logger(__name__)
logger.info(
    "Starting upload",
    extra={'file_path': '/tmp/data.parquet', 'size_mb': 15.3}
)
```

**Output**:
```json
{
  "timestamp": "2026-02-10 14:23:45",
  "level": "INFO",
  "logger": "dags.01_ingest",
  "message": "Starting upload",
  "file_path": "/tmp/data.parquet",
  "size_mb": 15.3
}
```

**Benefits**:
- **Observability**: Easy parsing by Datadog, CloudWatch, ELK
- **Debugging**: Contextual information in every log
- **Professionalism**: Production-grade logging

#### Example: GCP Helpers

Instead of reimplementing GCS upload logic:

```python
# ❌ BAD: Repeated in every DAG
from google.cloud import storage
client = storage.Client()
bucket = client.bucket('my-bucket')
blob = bucket.blob('path/to/file')
blob.upload_from_filename('/tmp/data.parquet')
# ... no retry logic, no error handling ...
```

Use helper with automatic retries:

```python
# ✅ GOOD: Reusable with built-in retry
from utils.gcp_helpers import upload_to_gcs

upload_to_gcs(
    local_path='/tmp/data.parquet',
    bucket_name='my-bucket',
    blob_name='raw/green_taxi/2019-01/data.parquet'
)
# Automatically retries on transient failures
```

**Location**: `airflow/utils/gcp_helpers.py`, `airflow/utils/logger.py`

---

### Summary: Code Quality Comparison

| Aspect | Without Patterns | With Production Patterns |
|--------|------------------|--------------------------|
| **Lines per DAG** | 500+ lines | 100-150 lines |
| **Code Duplication** | High (copy-paste) | Minimal (DRY) |
| **Testability** | Hard to test | Unit testable |
| **Maintainability** | Update 10 files | Update 1 file |
| **Debugging** | Print statements | Structured logs |
| **Professionalism** | Bootcamp student | Senior engineer |

**What Recruiters See**:

❌ **Without patterns**: "This person copies from tutorials"
✅ **With patterns**: "This person writes production code"

---

## 📁 Project Structure

```
05-data-platforms/
├── README.md                      # This file
├── SETUP.md                       # English setup guide
├── SETUP_FR.md                    # French setup guide
├── GUIDE_FR.md                    # French step-by-step tutorial
├── .env.example                   # Environment variables template
├── .gitignore
├── docker-compose.yml             # Airflow services
├── requirements.txt               # Python dependencies
│
├── airflow/
│   ├── dags/
│   │   ├── 01_ingest_nyc_taxi.py          # Incremental data ingestion
│   │   ├── 02_transform_dbt.py            # dbt orchestration
│   │   └── 03_quality_monitoring.py       # Data quality checks
│   │
│   ├── plugins/
│   │   └── operators/
│   │       ├── __init__.py
│   │       └── data_quality_operator.py   # Custom quality check operator
│   │
│   ├── config/
│   │   ├── airflow.cfg                    # Airflow configuration
│   │   └── connections.json.example       # Connection templates
│   │
│   └── utils/
│       ├── __init__.py
│       ├── logger.py                      # Structured JSON logging
│       ├── gcp_helpers.py                 # GCS/BigQuery utilities
│       └── config_loader.py               # Centralized config
│
├── dbt/
│   └── taxi_analytics/                    # dbt project (from Module 4)
│
├── tests/
│   ├── test_dag_integrity.py              # DAG validation tests
│   └── test_data_quality.py               # Quality operator tests
│
└── docs/
    ├── architecture.md                    # Detailed architecture
    ├── patterns.md                        # Pattern explanations
    └── diagrams/
        └── pipeline-flow.png              # Visual diagrams
```

---

## 🔧 Prerequisites

- **Google Cloud Platform Account** with billing enabled
- **Docker** and **Docker Compose** installed
- **dbt project** from Module 4 (or use the included template)
- **Basic knowledge** of SQL, Python, and command line

---

## 🚀 Quick Start

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd 05-data-platforms
```

### 2. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your values
nano .env
```

Required variables:
- `GCP_PROJECT_ID`: Your GCP project ID
- `GCS_BUCKET_NAME`: GCS bucket name (will be created)
- `GCP_CREDENTIALS_PATH`: Path to service account key

### 3. Add GCP Credentials

```bash
# Download service account key from GCP Console
# Place it in the config directory
cp ~/Downloads/gcp-key.json airflow/config/gcp-credentials.json
```

### 4. Start Airflow

```bash
# Start all services
docker compose up -d

# Check logs
docker compose logs -f airflow-scheduler
```

### 5. Access Airflow UI

Open http://localhost:8080

- **Username**: `admin` (or from `.env`)
- **Password**: `admin` (or from `.env`)

### 6. Trigger Backfill

```bash
# Trigger ingestion for 2019
docker exec -it airflow-scheduler \
  airflow dags backfill 01_ingest_nyc_taxi_incremental \
  --start-date 2019-01-01 \
  --end-date 2019-12-31
```

**For detailed setup instructions**, see [SETUP.md](SETUP.md) (English) or [SETUP_FR.md](SETUP_FR.md) (French).

---

## 📊 Pipeline Details

### DAG 1: Incremental Data Ingestion

**Schedule**: Monthly (`@monthly`)
**Catchup**: Enabled (supports backfill)

**Tasks**:
1. **Download Data**: Fetch Parquet file from NYC TLC
2. **Get GCS Destination**: Generate organized path
3. **Upload to GCS**: Store in `raw/green_taxi/YYYY-MM/`
4. **Load to BigQuery**: Partitioned table by `lpep_pickup_datetime`
5. **Cleanup**: Remove temporary local file

**Key Features**:
- Incremental loading based on `execution_date`
- Idempotent (re-run safe)
- Partitioned and clustered BigQuery tables
- Automatic retry with exponential backoff

### DAG 2: dbt Transformations

**Schedule**: Monthly (matches ingestion)
**Dependencies**: Waits for DAG 1 via `ExternalTaskSensor`

**Tasks**:
1. **Wait for Ingestion**: Ensure data is loaded
2. **Install Dependencies**: `dbt deps`
3. **Run Staging Models**: Basic cleaning
4. **Test Staging**: Validate staging layer
5. **Run Core Models**: Business logic
6. **Test Core**: Final validation
7. **Generate Docs**: dbt documentation

**Key Features**:
- Layered transformation (staging → core)
- Test-driven development
- Cross-DAG dependencies

### DAG 3: Data Quality Monitoring

**Schedule**: Monthly
**Dependencies**: Waits for DAG 1

**Checks**:
1. **Freshness**: Data loaded for expected month
2. **Completeness**: No NULL pickup/dropoff times
3. **Validity**: Reasonable trip distances (<200 miles)
4. **Validity**: Positive passenger counts
5. **Validity**: Reasonable fare amounts ($0-$1000)
6. **Validity**: Valid location IDs

**Key Features**:
- Custom `DataQualityOperator`
- Parallel execution of checks
- Zero retries (fail fast)
- Structured logging for monitoring

---

## 💡 Key Learnings

| Concept | Implementation | Why It Matters |
|---------|---------------|----------------|
| **Incremental Loading** | Date-based partitioning | Efficiency, idempotence |
| **Cross-DAG Dependencies** | ExternalTaskSensor | Decoupling, scalability |
| **Data Quality** | Automated checks | Trust, reliability |
| **Custom Operators** | DataQualityOperator | Reusability, DRY |
| **Structured Logging** | JSON format | Observability, debugging |
| **Configuration Management** | Environment variables | Security, flexibility |

---

## 🚀 Next Steps

### Production Enhancements

- [ ] **Deploy to Cloud Composer** (managed Airflow on GCP)
- [ ] **Add Great Expectations** for advanced data quality
- [ ] **Implement Alerting** (Slack, email, PagerDuty)
- [ ] **Add More Data Sources** (e.g., IDFM API for Paris transport)
- [ ] **CI/CD Pipeline** (GitHub Actions for DAG testing)
- [ ] **Monitoring Dashboard** (Grafana + Prometheus)

### Advanced Features

- [ ] **SCD Type 2** with dbt snapshots
- [ ] **Incremental dbt models** for performance
- [ ] **Dynamic DAG generation** for multiple data sources
- [ ] **Airflow Variables** and **Connections** UI
- [ ] **Data lineage visualization** with OpenLineage

---

## 📚 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Google Cloud Composer](https://cloud.google.com/composer)
- [DataTalks.Club DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)

---

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Ellie** - Data Engineering Zoomcamp 2026

- Portfolio: [Your Portfolio URL]
- LinkedIn: [Your LinkedIn]
- GitHub: [@YourGitHub](https://github.com/YourGitHub)

---

**⭐ If this project helped you, please give it a star!**
