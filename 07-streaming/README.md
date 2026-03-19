# Module 7 — Stream Processing with Kafka (Redpanda) + PyFlink

Data Engineering Zoomcamp 2026 | Green Taxi Trip Data — October 2025

## Stack

- Redpanda v25.3.9 (Kafka-compatible broker)
- Apache Flink 2.2.0
- PyFlink (Python API for Flink)
- PostgreSQL 18
- Python 3.12 + kafka-python + pandas + pyarrow

## Project Structure

```
07-streaming/
├── .venv/                          # Virtual environment (gitignored)
└── workshop/
    ├── data/                       # Parquet data files (gitignored)
    ├── src/
    │   ├── consumers/              # Workshop original consumers (rides topic)
    │   ├── producers/              # Workshop original producers (rides topic)
    │   ├── job/
    │   │   ├── hw/                 # Homework Flink jobs + producer/consumer
    │   │   │   ├── producer.py     # Green taxi producer (green-trips topic)
    │   │   │   ├── consumer.py     # Green taxi consumer (green-trips topic)
    │   │   │   ├── job_q4_tumbling_pickup.py
    │   │   │   ├── job_q5_session_window.py
    │   │   │   └── job_q6_tumbling_tips.py
    │   │   ├── aggregation_job.py  # Workshop original jobs
    │   │   └── pass_through_job.py
    │   └── models.py
    ├── docker-compose.yml
    └── Dockerfile.flink
```

> **Important**: Flink jobs must be placed in `src/job/hw/` — this directory is mounted into the Flink containers via the volume `./src/:/opt/src`. All edits must be made there, not in any other location.

## Setup

### 1. Port conflicts

The original `docker-compose.yml` uses ports 8081 and 8082, which may conflict with other running services. Update before starting:

```yaml
redpanda:
  ports:
    - 8085:8082    # was 8082:8082

jobmanager:
  ports:
    - "8083:8081"  # was 8081:8081
```

The Flink UI will be available at http://localhost:8083

### 2. Python environment

```bash
cd 07-streaming
python3 -m venv .venv
source .venv/bin/activate
pip install kafka-python pandas pyarrow
```

### 3. Start the infrastructure

```bash
cd workshop
docker compose build
docker compose up -d
docker compose ps
```

Services started:
- Redpanda (Kafka): `localhost:9092`
- Flink Job Manager: `http://localhost:8083`
- PostgreSQL: `localhost:5432`

### 4. Download the dataset

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet -P data/
```

Dataset: **49,416 rows** of Green Taxi trips from October 2025.

### 5. Create the Redpanda topic

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

### 6. Create PostgreSQL tables

```bash
docker exec -it workshop-postgres-1 psql -U postgres -d postgres -c "
CREATE TABLE tumbling_window_trips (
    window_start TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
CREATE TABLE session_window_trips (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);
CREATE TABLE tumbling_window_tips (
    window_start TIMESTAMP,
    total_tip DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);"
```

---

## Questions 1–3: Kafka Producer/Consumer

### Q1 — Redpanda version

```bash
docker exec -it workshop-redpanda-1 rpk version
```

**Answer: v25.3.9** ✅

### Q2 — Sending data to Redpanda

The producer (`src/job/hw/producer.py`) reads the parquet file and sends each row to the `green-trips` topic.

**Important fix**: The dataset contains `NaN` values in `passenger_count`. Flink cannot parse `NaN` in JSON. Replace with `None` (serialized as `null`) before sending:

```python
df = df.where(pd.notnull(df), None)
```

```bash
python3 src/job/hw/producer.py
```

**Answer: 10 seconds** ✅ (~6-8s in practice, falls in the "10 seconds" range)

### Q3 — Trips with distance > 5km

```bash
python3 src/job/hw/consumer.py
```

**Answer: 8506** ✅

---

## Questions 4–6: PyFlink

### Fix applied to all Flink jobs

Even with the producer fix, older messages in the topic may still contain `NaN`. All job source DDLs include:

```python
'json.ignore-parse-errors' = 'true'
```

### Generic command to submit a Flink job

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/hw/<job_file>.py
```

### Clean reset between jobs

```bash
# Delete and recreate the topic to avoid duplicates
docker exec -it workshop-redpanda-1 rpk topic delete green-trips
docker exec -it workshop-redpanda-1 rpk topic create green-trips
python3 src/job/hw/producer.py

# Truncate the relevant PostgreSQL table
docker exec -it workshop-postgres-1 psql -U postgres -d postgres -c "TRUNCATE <table>;"
```

### Q4 — Tumbling window (5 min) by PULocationID

Job: `src/job/hw/job_q4_tumbling_pickup.py`

5-minute tumbling window counting trips per `PULocationID`.

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/hw/job_q4_tumbling_pickup.py
```

```sql
SELECT PULocationID, num_trips
FROM tumbling_window_trips
ORDER BY num_trips DESC
LIMIT 3;
```

```
 pulocationid | num_trips
--------------+-----------
           74 |        15
           74 |        14
           74 |        13
```

**Answer: 74** ✅

### Q5 — Session window by PULocationID

Job: `src/job/hw/job_q5_session_window.py`

Session window with a 5-minute gap, partitioned by `PULocationID`.

#### ⚠️ Key lesson: PARTITION BY is required

**Without `PARTITION BY`**: Flink creates a single global session across all locations — any incoming event from any location keeps the window open. This produces a single massive session with hundreds of trips (401 in our case), which does not match any answer choice.

**With `PARTITION BY PULocationID`**: Flink creates an independent session per location — the window for location 74 only closes after 5 minutes of inactivity at that specific location. This produces realistic session sizes.

```sql
FROM TABLE(
    SESSION(TABLE green_trips PARTITION BY PULocationID,
            DESCRIPTOR(event_timestamp),
            INTERVAL '5' MINUTE)
)
```

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/hw/job_q5_session_window.py
```

```sql
SELECT PULocationID, num_trips
FROM session_window_trips
ORDER BY num_trips DESC
LIMIT 3;
```

```
 pulocationid | num_trips
--------------+-----------
           74 |        81
           74 |        72
           74 |        71
```

**Answer: 81** ✅

### Q6 — Tumbling window (1 hour) — Total tips

Job: `src/job/hw/job_q6_tumbling_tips.py`

1-hour tumbling window summing `tip_amount` across all locations.

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/hw/job_q6_tumbling_tips.py
```

```sql
SELECT window_start, total_tip
FROM tumbling_window_tips
ORDER BY total_tip DESC
LIMIT 3;
```

```
    window_start     | total_tip
---------------------+-----------
 2025-10-16 18:00:00 |    510.86
 2025-10-30 16:00:00 |    494.41
 2025-10-09 18:00:00 |    472.01
```

**Answer: 2025-10-16 18:00:00** ✅

---

## Answers Summary

| # | Question | Answer |
|---|----------|--------|
| 1 | Redpanda version | **v25.3.9** |
| 2 | Time to send data | **10 seconds** |
| 3 | Trips with distance > 5km | **8506** |
| 4 | Top PULocationID in 5-min tumbling window | **74** |
| 5 | Trips in longest session | **81** |
| 6 | Hour with highest total tips | **2025-10-16 18:00:00** |

---

## Troubleshooting

**TaskManager crashes**: The task manager can crash if too many jobs fail in a restart loop (OOM). Full restart:
```bash
docker compose down
docker compose up -d
```

**Topic lost after restart**: `docker compose down` removes volumes. Recreate the topic and resend data after restarting.

**NaN in JSON**: Always add `df = df.where(pd.notnull(df), None)` in the producer AND `'json.ignore-parse-errors' = 'true'` in the Flink source DDL.

**Topic not deleted immediately**: Redpanda's delete is asynchronous. Add a short sleep between delete and create:
```bash
docker exec -it workshop-redpanda-1 rpk topic delete green-trips
sleep 3
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```
