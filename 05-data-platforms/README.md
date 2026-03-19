# 🚕 Module 5: NYC Taxi Data Platform

**Project**: Production-grade data pipeline for NYC Taxi data analysis
**Author**: Ellie - Data Engineering Zoomcamp 2026
**Stack**: Airflow + dbt + BigQuery + GCS

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup Guide](#setup-guide)
- [Running the Pipeline](#running-the-pipeline)
- [DAGs Description](#dags-description)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)

---

## 🎯 Overview

This project implements a complete **ELT pipeline** for NYC Taxi data:

1. **Extract & Load**: Download Parquet files from NYC TLC → Upload to GCS → Load to BigQuery
2. **Transform**: dbt models for data transformation (bronze → silver → gold)
3. **Quality**: Automated data quality checks

**Key Features**:
- ✅ Incremental backfill (2019-2020 data)
- ✅ Partitioned & clustered BigQuery tables
- ✅ Modular Python code with custom operators
- ✅ Production-ready logging & monitoring
- ✅ No JSON credentials (uses gcloud auth)

---

## 🏗️ Architecture

```
NYC TLC API
    ↓ (monthly Parquet files)
Airflow DAG 1: Ingestion
    ↓
Google Cloud Storage (raw/)
    ↓
BigQuery (raw_data.green_taxi)
    ↓
Airflow DAG 2: dbt Transformation
    ↓
BigQuery (staging.*, analytics.*)
    ↓
Airflow DAG 3: Quality Monitoring
    ↓
Quality Reports
```

---

## 📦 Prerequisites

### Required Tools

- **Docker** & Docker Compose
- **Python 3.11+** with venv
- **gcloud CLI** (Google Cloud SDK)
- **Git**

### GCP Resources

- GCP Project with billing enabled
- BigQuery API enabled
- Cloud Storage API enabled
- Service Account with roles:
  - BigQuery Data Editor
  - BigQuery Job User
  - Storage Object Admin

---

## 🚀 Setup Guide

### 1. Clone the Repository

```bash
git clone <your-repo>
cd 05-data-platforms
```

---

### 2. GCP Authentication Setup (NO JSON KEYS!)

**This project uses gcloud auth instead of service account JSON keys.**

```bash
# Step 1: Install gcloud CLI (if not already installed)
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Step 2: Login with your Google account
gcloud auth login

# Step 3: Set your GCP project
gcloud config set project YOUR-PROJECT-ID

# Step 4: Create Application Default Credentials
gcloud auth application-default login

# Step 5: Verify it works
bq ls  # Should list datasets (empty at first)
```

**Why no JSON keys?**
- ✅ More secure (no credentials file to leak)
- ✅ Easier to manage
- ✅ Production-ready pattern
- ✅ Works in Docker via volume mount

---

### 3. Create GCP Resources

```bash
# Create GCS bucket
gcloud storage buckets create gs://YOUR-PROJECT-ID-data \
  --project=YOUR-PROJECT-ID \
  --location=europe-west1

# Create BigQuery datasets
bq mk --dataset --location=EU --project_id=YOUR-PROJECT-ID raw_data
bq mk --dataset --location=EU --project_id=YOUR-PROJECT-ID staging
bq mk --dataset --location=EU --project_id=YOUR-PROJECT-ID analytics
```

---

### 4. Configure Environment

```bash
# Copy example env file
cp .env.example .env

# Edit .env with your project details
nano .env
```

**Required variables**:
```bash
GCP_PROJECT_ID=your-project-id
GCP_REGION=europe-west1
GCS_BUCKET_NAME=your-project-id-data

BQ_DATASET_RAW=raw_data
BQ_DATASET_STAGING=staging
BQ_DATASET_ANALYTICS=analytics

DATA_START_DATE=2019-01-01
DATA_END_DATE=2020-12-31
TAXI_TYPE=green
```

**Note**: NO `GOOGLE_APPLICATION_CREDENTIALS` needed!

---

### 5. Setup Python Virtual Environment (Optional)

For local development and IDE autocomplete:

```bash
# Create venv
make venv

# Activate
source .venv/bin/activate

# Install dependencies
pip install -r requirements-dev.txt
```

---

### 6. Configure dbt

```bash
# Edit dbt profiles
nano dbt/taxi_analytics/profiles.yml
```

**Correct configuration (uses oauth)**:
```yaml
taxi_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth  # ← Uses gcloud auth
      project: YOUR-PROJECT-ID
      dataset: staging
      threads: 4
      location: EU
      # NO keyfile parameter!
```

---

### 7. Start Airflow

```bash
# Start all services
make start

# The Makefile automatically:
# 1. Checks .env configuration
# 2. Sets gcloud credentials permissions (chmod 644)
# 3. Starts Docker containers
```

**Access Airflow UI**: http://localhost:8082
- Username: `admin`
- Password: `admin` (or check `.env`)

---

## 🎮 Running the Pipeline

### Enable DAGs

1. Go to http://localhost:8082
2. Toggle ON the following DAGs:
   - `01_ingest_nyc_taxi_incremental`
   - `02_transform_dbt_models`
   - `03_quality_monitoring`

### Monitor Execution

The pipeline will automatically:
1. **Backfill** all months from 2019-01 to 2020-12
2. Run **3 concurrent** monthly ingestions
3. Transform data with dbt after ingestion completes
4. Run quality checks

**Grid View** shows execution timeline.

---

## 📊 DAGs Description

### DAG 1: `01_ingest_nyc_taxi_incremental`

**Purpose**: Incremental data ingestion

**Tasks**:
1. `download_data`: Download monthly Parquet from NYC TLC
2. `get_gcs_destination`: Generate GCS path
3. `upload_to_gcs`: Upload to Cloud Storage
4. `load_to_bigquery`: Load to BigQuery (partitioned + clustered)
5. `cleanup_local_file`: Remove temporary files

**Schedule**: `@monthly`
**Catchup**: `True` (enables backfill)
**Max Active Runs**: 3

---

### DAG 2: `02_transform_dbt_models`

**Purpose**: Data transformation with dbt

**Tasks**:
1. `wait_for_ingestion`: Wait for DAG 1 to complete
2. `dbt_deps`: Install dbt dependencies
3. `dbt_run_staging`: Transform staging models
4. `dbt_test_staging`: Test staging models
5. `dbt_run_core`: Transform core analytics models
6. `dbt_test_core`: Test core models
7. `dbt_docs_generate`: Generate documentation

**Schedule**: `@monthly`
**Depends on**: DAG 1 completion

---

### DAG 3: `03_quality_monitoring`

**Purpose**: Data quality monitoring

**Tasks**:
1. `wait_for_ingestion`: Wait for DAG 1
2. `check_data_freshness`: Verify recent data
3. `check_no_null_pickup_datetime`: Null checks
4. `check_trip_distances`: Distance validation
5. `check_passenger_count`: Passenger validation
6. `check_fare_amounts`: Fare validation
7. `check_location_ids`: Location validation
8. `log_quality_summary`: Summary report

**Schedule**: `@monthly`

---

## 🛠️ Troubleshooting

### Common Issues

#### 1. **Credentials Not Found**

```
Error: CLOUD_SDK_MISSING_CREDENTIALS
```

**Solution**:
```bash
gcloud auth application-default login
make restart  # Restart Airflow
```

---

#### 2. **Bucket Does Not Exist**

```
Error: 404 The specified bucket does not exist
```

**Solution**:
```bash
gcloud storage buckets create gs://YOUR-PROJECT-ID-data \
  --project=YOUR-PROJECT-ID \
  --location=europe-west1
```

---

#### 3. **Dataset Not Found**

```
Error: 404 Not found: Dataset raw_data
```

**Solution**:
```bash
bq mk --dataset --location=EU --project_id=YOUR-PROJECT-ID raw_data
bq mk --dataset --location=EU --project_id=YOUR-PROJECT-ID staging
bq mk --dataset --location=EU --project_id=YOUR-PROJECT-ID analytics
```

---

#### 4. **Schema Mismatch Error**

```
Error: Field ehail_fee has changed type from FLOAT to INTEGER
```

**Solution**:
```bash
# Delete and recreate table
bq rm -f -t YOUR-PROJECT-ID:raw_data.green_taxi

# Clear failed tasks in Airflow UI
# Tasks will retry and recreate table with correct schema
```

---

#### 5. **Permission Denied in Docker**

```
Error: Permission denied: '/home/airflow/.config/gcloud/...'
```

**Solution**:
```bash
chmod 644 ~/.config/gcloud/application_default_credentials.json
make restart
```

The `make start` command now does this automatically!

---

### Useful Commands

```bash
# View logs
make logs

# Stop Airflow
make stop

# Restart Airflow
make restart

# Clean up (stop + remove containers)
make clean

# Complete cleanup (including volumes)
make clean-all

# Shell into scheduler
make shell-scheduler

# Shell into webserver
make shell-webserver
```

---

## 📁 Project Structure

```
05-data-platforms/
├── airflow/
│   ├── dags/
│   │   ├── 01_ingest_nyc_taxi.py       # Ingestion DAG
│   │   ├── 02_transform_dbt.py         # Transformation DAG
│   │   └── 03_quality_monitoring.py    # Quality DAG
│   ├── plugins/
│   │   └── operators/
│   │       └── data_quality_operator.py # Custom operator
│   ├── utils/
│   │   ├── logger.py                    # Structured logging
│   │   ├── config_loader.py             # Config management
│   │   └── gcp_helpers.py               # GCP utilities
│   └── config/
│       └── logging.json                 # Logging config
├── dbt/
│   └── taxi_analytics/
│       ├── models/
│       │   ├── staging/                 # Staging models
│       │   └── core/                    # Core analytics
│       ├── tests/                       # dbt tests
│       └── profiles.yml                 # dbt connection config
├── docker-compose.yml                   # Airflow services
├── Makefile                             # Automation commands
├── requirements.txt                     # Python dependencies (Docker)
├── requirements-dev.txt                 # Dev dependencies (venv)
├── .env                                 # Environment variables
└── README.md                            # This file
```

---

## 🎓 Learning Outcomes

This project demonstrates:

✅ **Production-grade Airflow** setup
✅ **Incremental data loading** with backfill
✅ **BigQuery optimization** (partitioning & clustering)
✅ **dbt best practices** (staging → core layers)
✅ **Custom Airflow operators** for reusability
✅ **Modern GCP authentication** (no JSON keys)
✅ **Docker-based development** workflow
✅ **Data quality monitoring** automation

---

## 📚 Resources

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Google Cloud Authentication](https://cloud.google.com/docs/authentication/application-default-credentials)

---

## 🚀 Next Steps

1. **Add Great Expectations** for advanced data quality
2. **Implement alerting** (Slack, email)
3. **Add Terraform** for infrastructure as code
4. **Deploy to Cloud Composer** (managed Airflow)
5. **Add incremental dbt models**
6. **Implement data lineage tracking**

---

## 📧 Contact

**Ellie Pascaud**
Data Engineering Zoomcamp 2026
Transitioning from Financial Controller to Analytics Engineer

---

**⭐ If this helps you, please star the repository!**
