# Module 3 Homework: Data Warehousing with BigQuery

**Data Engineering Zoomcamp 2026** - Module 3
**Author:** Ellie Pascaud
**Date:** February 2026

## 📋 Project Overview

This project demonstrates data warehousing fundamentals using Google BigQuery and Google Cloud Storage (GCS). The homework involves working with 20+ million NYC Yellow Taxi trip records from January-June 2024 to understand:

- External vs materialized tables
- Query cost optimization
- Partitioning and clustering strategies
- Columnar storage benefits

## 🏗️ Architecture
```
NYC Taxi Data (Parquet files)
    ↓
Google Cloud Storage Bucket
    ↓
BigQuery External Table (references GCS)
    ↓
BigQuery Materialized Table (data in BigQuery)
    ↓
BigQuery Partitioned & Clustered Table (optimized)
```

## 🛠️ Technologies Used

- **Cloud Platform:** Google Cloud Platform (GCP)
- **Data Warehouse:** BigQuery
- **Storage:** Google Cloud Storage
- **Language:** Python 3.x, SQL
- **Authentication:** Application Default Credentials (gcloud SDK)

## 📂 Project Structure
```
03-data-warehouse/
├── load_yellow_taxi_data.py    # Script to download and upload data to GCS
├── requirements.txt             # Python dependencies
├── queries/                     # SQL queries for homework questions
│   ├── question1.sql
│   ├── question2.sql
│   ├── question3.sql
│   ├── question4.sql
│   ├── question5.sql
│   ├── question6.sql
│   ├── question7.md
│   ├── question8.md
│   └── question9.sql
├── screenshots/                 # Query results and estimates
│   ├── question1_count_result.png
│   ├── question2_external_estimate.png
│   └── ...
├── setup_bigquery.sql          # BigQuery setup (datasets, tables)
├── README.md
└── .gitignore
```

## ⚙️ Setup Instructions

### Prerequisites

- Google Cloud Platform account with Free Trial ($300 credit)
- gcloud SDK installed and configured
- Python 3.8+

### 1. Authentication Setup
```bash
# Install gcloud SDK (if not already installed)
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Authenticate with your Google account
gcloud auth login

# Set your project
gcloud config set project de-zoomcamp-module3-486909

# Setup Application Default Credentials (for Python client libraries)
gcloud auth application-default login
```

**Note:** This project uses Application Default Credentials instead of service account JSON keys, following Google's security best practices.

### 2. Python Environment Setup
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Data Upload to GCS
```bash
# Run the data loading script
# This will download 6 parquet files and upload them to GCS
python load_yellow_taxi_data.py
```

**Expected output:**
- Downloads 6 parquet files (~500 MB total)
- Uploads to GCS bucket: `gs://dezoomcamp-hw3-2026-ellie/`
- Duration: ~10 minutes

### 4. BigQuery Setup
```bash
# Create dataset and tables in BigQuery
# Run the setup_bigquery.sql file in BigQuery console
# Or use bq command-line tool:
bq query < setup_bigquery.sql
```

## 📊 Dataset Information

- **Source:** NYC Taxi & Limousine Commission
- **Data Period:** January 2024 - June 2024
- **Record Count:** 20,332,093 rows
- **File Format:** Parquet
- **Total Size:** ~3 GB (compressed)

**Data Schema:**
- VendorID
- tpep_pickup_datetime
- tpep_dropoff_datetime
- PULocationID
- DOLocationID
- fare_amount
- ... (19 columns total)

## 🎯 Homework Questions & Answers

### Question 1: Counting Records
**Query:** Count total records in 2024 Yellow Taxi Data
**Answer:** 20,332,093 records

### Question 2: Data Read Estimation
**Query:** Count distinct PULocationIDs on both external and materialized tables
**Answer:**
- External Table: ~2.14 GB estimated bytes
- Materialized Table: ~155 MB estimated bytes

**Key Learning:** Materialized tables in BigQuery use optimized columnar storage format, significantly reducing bytes scanned.

### Question 3: Understanding Columnar Storage
**Question:** Why do two columns require more bytes than one column?
**Answer:** BigQuery is a columnar database and only scans specific columns requested in the query. Querying two columns requires reading more data than one column.

**Evidence:**
- 1 column (PULocationID): ~X MB
- 2 columns (PULocationID + DOLocationID): ~2X MB

### Question 4: Zero Fare Trips
**Query:** Count records with fare_amount = 0
**Answer:** [Your result here] records

### Question 5: Partitioning and Clustering Strategy
**Question:** Best optimization for queries filtering on tpep_dropoff_datetime and ordering by VendorID?
**Answer:** Partition by tpep_dropoff_datetime and Cluster on VendorID

**Reasoning:**
- **Partition by date:** Reduces scanned data when filtering by date ranges
- **Cluster by VendorID:** Improves performance for ORDER BY operations

### Question 6: Partition Benefits
**Query:** Compare bytes scanned for non-partitioned vs partitioned table
**Answer:**
- Non-partitioned table: ~310 MB
- Partitioned table: ~26-28 MB

**Performance gain:** ~91% reduction in bytes scanned

### Question 7: External Table Storage Location
**Answer:** GCP Bucket (Google Cloud Storage)

External tables reference data in GCS rather than storing it in BigQuery.

### Question 8: Clustering Best Practice
**Question:** Should you always cluster your data?
**Answer:** False

**Explanation:** Clustering is beneficial for large tables with clear query patterns, but adds overhead for small tables or unpredictable queries.

### Question 9: COUNT(*) Optimization (Bonus)
**Query:** Why does COUNT(*) scan minimal bytes?
**Answer:** BigQuery reads only table metadata for COUNT(*), not actual data rows.

**Estimated bytes:** ~0 MB

## 💡 Key Learnings

1. **External vs Materialized Tables:**
   - External tables are cheaper (storage) but slower (queries)
   - Materialized tables enable partitioning and clustering

2. **Cost Optimization:**
   - Partitioning can reduce query costs by 90%+
   - Always check estimated bytes before running queries

3. **Columnar Storage:**
   - Select only needed columns to minimize costs
   - BigQuery's columnar format excels at analytical queries

4. **Authentication Best Practices:**
   - Application Default Credentials > Service Account JSON keys
   - Reduces security risks and simplifies local development

## 📈 Query Performance Comparison

| Table Type | Query Type | Bytes Scanned | Performance |
|------------|-----------|---------------|-------------|
| External | COUNT DISTINCT | 2.14 GB | Baseline |
| Materialized | COUNT DISTINCT | 155 MB | 14× faster |
| Non-partitioned | Date filter | 310 MB | Baseline |
| Partitioned | Date filter | 26 MB | 12× faster |

## 🔒 Security Notes

- No service account JSON keys committed to repository
- Application Default Credentials used for local development
- `.gitignore` configured to prevent credential leaks
- Public access prevention enabled on GCS bucket


## 📚 Resources

- [NYC Taxi Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)

## 🙏 Acknowledgments

- DataTalks.Club for the excellent Zoomcamp curriculum
- NYC TLC for providing open taxi data

