# Module 6 — Apache Spark Homework

Data Engineering Zoomcamp 2026 | Batch Processing with Apache Spark

## Stack

- Apache Spark 3.5
- PySpark
- Jupyter Lab
- Docker

## Project Structure

```
module6-spark/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── notebooks/
│   └── homework.ipynb     # Interactive exploration
├── scripts/
│   └── homework.py        # Clean Python script
├── data/                  # gitignored — see Data Setup
└── README.md
```

## Setup

### 1. Download data

```bash
mkdir -p data

# Yellow Taxi November 2025
wget -P data https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet

# Zone lookup
wget -P data https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

### 2. Build and start the container

```bash
docker compose up --build
```

### 3. Open Jupyter Lab

Go to http://localhost:8888 and use token: `spark2026`

Open `notebooks/homework.ipynb` and run cells sequentially.

### 4. (Optional) Run the script directly

```bash
docker compose exec spark-notebook python scripts/homework.py
```

### 5. Spark UI

While a Spark session is active, the Spark UI is available at:
http://localhost:4040

## Homework Questions

| # | Question | Answer |
|---|----------|--------|
| 1 | Spark version | *3.5.0* |
| 2 | Average parquet file size after repartition(4) | ~25 MB |
| 3 | Trips starting on November 15th | *162,604* |
| 4 | Longest trip in hours | *90.6 hours* |
| 5 | Spark UI port | 4040 |
| 6 | Least frequent pickup zone | *Governor's Island/Ellis Island/Liberty Island* |

## Key Concepts Practiced

- Local Spark session setup
- Reading/writing Parquet files
- DataFrame repartitioning
- Filtering and aggregating with PySpark functions
- Joining DataFrames (taxi trips × zone lookup)
- Spark UI for job monitoring
