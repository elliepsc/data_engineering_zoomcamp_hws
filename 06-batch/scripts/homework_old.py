"""
Module 6 Homework — Apache Spark
Data Engineering Zoomcamp 2026

Usage:
    python scripts/homework.py

Prerequisites:
    - data/yellow_tripdata_2025-11.parquet
    - data/taxi_zone_lookup.csv
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("homework6") \
        .getOrCreate()


def q1_spark_version(spark):
    print("\n" + "="*50)
    print("Q1 — Spark version")
    print("="*50)
    print(f"Spark version: {spark.version}")


def q2_avg_parquet_size(df, output_dir="data/yellow_2025_11_repartitioned"):
    print("\n" + "="*50)
    print("Q2 — Average parquet file size")
    print("="*50)

    df.repartition(4).write.parquet(output_dir, mode="overwrite")

    parquet_files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
    sizes_mb = [
        os.path.getsize(os.path.join(output_dir, f)) / (1024 * 1024)
        for f in parquet_files
    ]

    avg_mb = sum(sizes_mb) / len(sizes_mb)
    print(f"Number of parquet files: {len(parquet_files)}")
    print(f"File sizes (MB): {[round(s, 2) for s in sizes_mb]}")
    print(f"Average size: {avg_mb:.2f} MB")


def q3_count_nov15(df):
    print("\n" + "="*50)
    print("Q3 — Trips on November 15th")
    print("="*50)

    count = df.filter(
        F.to_date(F.col("tpep_pickup_datetime")) == "2025-11-15"
    ).count()

    print(f"Trips starting on November 15th: {count:,}")


def q4_longest_trip(df):
    print("\n" + "="*50)
    print("Q4 — Longest trip in hours")
    print("="*50)

    df_duration = df.withColumn(
        "duration_hours",
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        ) / 3600,
    )

    max_hours = df_duration.agg(F.max("duration_hours")).collect()[0][0]
    print(f"Longest trip: {max_hours:.1f} hours")


def q5_spark_ui():
    print("\n" + "="*50)
    print("Q5 — Spark UI port")
    print("="*50)
    print("Spark UI runs on port: 4040")
    print("Access at: http://localhost:4040")


def q6_least_frequent_zone(spark, df, zones_path="data/taxi_zone_lookup.csv"):
    print("\n" + "="*50)
    print("Q6 — Least frequent pickup zone")
    print("="*50)

    zones = spark.read.option("header", "true").csv(zones_path)

    pickup_counts = df.groupBy("PULocationID").count()

    result = (
        pickup_counts
        .join(zones, pickup_counts["PULocationID"] == zones["LocationID"], "left")
        .select("Zone", "count")
        .orderBy("count")
        .limit(10)
    )

    print("10 least frequent pickup zones:")
    result.show(truncate=False)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*50)
    print("Loading data...")
    print("="*50)
    df = spark.read.parquet("data/yellow_tripdata_2025-11.parquet")
    print(f"Total rows: {df.count():,}")

    q1_spark_version(spark)
    q2_avg_parquet_size(df)
    q3_count_nov15(df)
    q4_longest_trip(df)
    q5_spark_ui()
    q6_least_frequent_zone(spark, df)

    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()
