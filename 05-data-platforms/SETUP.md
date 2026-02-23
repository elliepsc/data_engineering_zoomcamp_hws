# Setup Guide - Airflow Data Platform

This guide walks you through setting up the Airflow Data Platform on your local machine.

## Prerequisites

### Required Software

- **Docker Desktop** (or Docker Engine + Docker Compose)
  - Version: 20.10+ recommended
  - Download: https://www.docker.com/products/docker-desktop

- **Google Cloud Platform Account**
  - Free tier available: https://cloud.google.com/free
  - Billing must be enabled

- **Git** (for cloning the repository)

### Required GCP Permissions

Your service account needs these roles:
- `BigQuery Admin` (or `BigQuery Data Editor` + `BigQuery Job User`)
- `Storage Admin` (or `Storage Object Admin`)

---

## Step 1: Clone Repository

```bash
git clone <your-repo-url>
cd 05-data-platforms
```

---

## Step 2: GCP Project Setup

### 2.1 Create GCP Project (if needed)

```bash
# Install gcloud CLI: https://cloud.google.com/sdk/docs/install

# Create project
gcloud projects create my-data-platform-project --name="Data Platform"

# Set as default
gcloud config set project my-data-platform-project

# Enable billing (required)
# Go to: https://console.cloud.google.com/billing
```

### 2.2 Enable Required APIs

```bash
# Enable BigQuery API
gcloud services enable bigquery.googleapis.com

# Enable Cloud Storage API
gcloud services enable storage.googleapis.com

# Enable Compute Engine API (for Cloud Composer if deploying later)
gcloud services enable compute.googleapis.com
```

### 2.3 Create Service Account

```bash
# Create service account
gcloud iam service-accounts create airflow-data-platform \
    --display-name="Airflow Data Platform Service Account"

# Grant BigQuery Admin role
gcloud projects add-iam-policy-binding my-data-platform-project \
    --member="serviceAccount:airflow-data-platform@my-data-platform-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

# Grant Storage Admin role
gcloud projects add-iam-policy-binding my-data-platform-project \
    --member="serviceAccount:airflow-data-platform@my-data-platform-project.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Create and download key
gcloud iam service-accounts keys create ~/gcp-key.json \
    --iam-account=airflow-data-platform@my-data-platform-project.iam.gserviceaccount.com
```

### 2.4 Create GCS Bucket

```bash
# Create bucket (name must be globally unique)
gsutil mb -p my-data-platform-project -l EU gs://my-airflow-data-platform-bucket

# Verify
gsutil ls
```

### 2.5 Create BigQuery Datasets

```bash
# Create raw data dataset
bq mk --dataset --location=EU my-data-platform-project:raw_data

# Create staging dataset
bq mk --dataset --location=EU my-data-platform-project:staging

# Create analytics dataset
bq mk --dataset --location=EU my-data-platform-project:analytics

# Verify
bq ls
```

---

## Step 3: Configure Environment

### 3.1 Copy Environment Template

```bash
cp .env.example .env
```

### 3.2 Edit `.env` File

```bash
nano .env  # or use your preferred editor
```

**Update these values**:

```bash
# GCP Configuration
GCP_PROJECT_ID=my-data-platform-project
GCP_REGION=europe-west1
GCS_BUCKET_NAME=my-airflow-data-platform-bucket

# Path to service account key
GCP_CREDENTIALS_PATH=./config/gcp-credentials.json

# BigQuery datasets (created in step 2.5)
BQ_DATASET_RAW=raw_data
BQ_DATASET_STAGING=staging
BQ_DATASET_ANALYTICS=analytics

# Airflow admin credentials (change for production!)
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# Data configuration
DATA_START_DATE=2019-01-01
DATA_END_DATE=2020-12-31
TAXI_TYPE=green

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

### 3.3 Add GCP Credentials

```bash
# Copy service account key to config directory
cp ~/gcp-key.json airflow/config/gcp-credentials.json

# Verify file exists
ls -lh airflow/config/gcp-credentials.json
```

---

## Step 4: Start Airflow

### 4.1 Set Airflow User ID (Linux/Mac)

```bash
# Required for proper file permissions
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

### 4.2 Start Docker Compose

```bash
# Pull images and start services
docker compose up -d

# This will start:
# - PostgreSQL (metadata database)
# - Airflow Webserver (UI)
# - Airflow Scheduler (orchestrator)
```

### 4.3 Monitor Startup

```bash
# Watch logs
docker compose logs -f

# Check service status
docker compose ps

# Expected output:
# NAME                  STATUS
# airflow-webserver     Up (healthy)
# airflow-scheduler     Up (healthy)
# airflow-postgres      Up (healthy)
```

**Startup takes 1-2 minutes**. Wait for all services to show "healthy".

---

## Step 5: Access Airflow UI

### 5.1 Open Browser

Navigate to: **http://localhost:8080**

### 5.2 Login

- **Username**: `admin` (from `.env`)
- **Password**: `admin` (from `.env`)

### 5.3 Verify DAGs

You should see 3 DAGs:
- `01_ingest_nyc_taxi_incremental`
- `02_transform_dbt_models`
- `03_quality_monitoring`

All should be **paused** (toggle is off).

---

## Step 6: Configure GCP Connection (Optional)

By default, Airflow uses credentials from `GOOGLE_APPLICATION_CREDENTIALS` environment variable. 

If you need to configure the connection manually:

1. Go to **Admin > Connections** in Airflow UI
2. Click **+** to add connection
3. Fill in:
   - **Connection Id**: `google_cloud_default`
   - **Connection Type**: `Google Cloud`
   - **Project Id**: `my-data-platform-project`
   - **Keyfile Path**: `/opt/airflow/config/gcp-credentials.json`
4. Click **Test** then **Save**

---

## Step 7: Test Single DAG Run

### 7.1 Enable Ingestion DAG

In Airflow UI:
1. Click on `01_ingest_nyc_taxi_incremental`
2. Toggle the **pause** button to **unpause**

### 7.2 Trigger Manual Run

1. Click **Trigger DAG** (play button)
2. Select execution date: `2019-01-01`
3. Click **Trigger**

### 7.3 Monitor Execution

1. Click on the DAG run to see task details
2. Click on individual tasks to see logs
3. Wait for all tasks to complete (green checkmarks)

**Expected duration**: 2-5 minutes (depends on download speed)

### 7.4 Verify in BigQuery

```bash
# Check table exists
bq ls raw_data

# Check row count
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) as cnt FROM `my-data-platform-project.raw_data.green_taxi`'

# Expected: ~50,000-100,000 rows for January 2019
```

---

## Step 8: Run Full Backfill (Optional)

To process all 24 months (2019-2020):

```bash
# Trigger backfill for entire date range
docker exec -it airflow-scheduler \
  airflow dags backfill 01_ingest_nyc_taxi_incremental \
  --start-date 2019-01-01 \
  --end-date 2020-12-31

# This will create 24 DAG runs (one per month)
# Expected duration: 30-60 minutes
```

Monitor progress in Airflow UI under **Browse > DAG Runs**.

---

## Step 9: Enable All DAGs

Once ingestion is working:

1. Enable `02_transform_dbt_models`
2. Enable `03_quality_monitoring`

These will automatically run after ingestion completes.

---

## Troubleshooting

### Docker Issues

**Problem**: Services won't start

```bash
# Check Docker daemon is running
docker ps

# Restart Docker Desktop
# Then try again:
docker compose down
docker compose up -d
```

**Problem**: Port 8080 already in use

```bash
# Find process using port 8080
lsof -i :8080  # Mac/Linux
netstat -ano | findstr :8080  # Windows

# Kill process or change port in docker-compose.yml
```

### GCP Authentication Issues

**Problem**: `google.auth.exceptions.DefaultCredentialsError`

```bash
# Verify credentials file exists
ls -lh airflow/config/gcp-credentials.json

# Verify JSON is valid
cat airflow/config/gcp-credentials.json | jq .

# Check environment variable in container
docker exec -it airflow-scheduler \
  echo $GOOGLE_APPLICATION_CREDENTIALS
```

### BigQuery Permission Issues

**Problem**: `Access Denied` when loading data

```bash
# Verify service account has correct roles
gcloud projects get-iam-policy my-data-platform-project \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:airflow-data-platform*"

# Re-grant permissions if needed
gcloud projects add-iam-policy-binding my-data-platform-project \
  --member="serviceAccount:airflow-data-platform@my-data-platform-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin"
```

### DAG Import Errors

**Problem**: DAGs not appearing in UI

```bash
# Check scheduler logs
docker compose logs airflow-scheduler | grep -i error

# Common issues:
# - Syntax error in DAG file
# - Missing Python dependencies
# - Import path issues

# Test DAG parsing
docker exec -it airflow-scheduler \
  airflow dags list
```

### Download Failures

**Problem**: NYC TLC download fails

- **Check URL**: Verify file exists for the date
  - Visit: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Network**: Check internet connection
- **Retry**: DAG is configured to retry 2 times automatically

---

## Clean Up

### Stop Services

```bash
# Stop containers (keeps data)
docker compose stop

# Remove containers (keeps volumes)
docker compose down

# Remove everything including data
docker compose down -v
```

### Delete GCP Resources

```bash
# Delete BigQuery datasets
bq rm -r -f my-data-platform-project:raw_data
bq rm -r -f my-data-platform-project:staging
bq rm -r -f my-data-platform-project:analytics

# Delete GCS bucket
gsutil rm -r gs://my-airflow-data-platform-bucket

# Delete service account
gcloud iam service-accounts delete \
  airflow-data-platform@my-data-platform-project.iam.gserviceaccount.com

# Delete project (if no longer needed)
gcloud projects delete my-data-platform-project
```

---

## Next Steps

- **Read GUIDE_FR.md** for detailed French tutorial
- **Customize DAGs** for your use case
- **Add more data sources** (see docs/patterns.md)
- **Deploy to Cloud Composer** for production

---

## Getting Help

- **Airflow Docs**: https://airflow.apache.org/docs/
- **GCP Docs**: https://cloud.google.com/docs
- **DataTalks.Club**: https://datatalks.club/
- **Open an Issue**: [Your repo issues page]
