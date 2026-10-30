"""
query_station_hour.py
---------------------
Starter analytical queries for team members to run against the curated
HDFS Parquet dataset using Spark SQL.

Usage inside Docker:
  docker exec -it airwatch-spark-master spark-submit /workspace/query_station_hour.py

Usage standalone:
  python query_station_hour.py --path data/station_hour_parquet
"""

import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Query curated Air Quality Parquet table in HDFS")
    parser.add_argument(
        "--path",
        default="hdfs://namenode:9000/data/air_quality/curated/station_hour/",
        help="Path to curated Parquet dataset"
    )
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("AirWatch-Team-Query") \
        .getOrCreate()

    # Read the partitioned dataset (partition pruning on year/month is automatic)
    print(f"Reading dataset from {args.path}...")
    df = spark.read.parquet(args.path)
    df.createOrReplaceTempView("station_hour")

    print("\n--- 1. Partition Pruning: Average PM2.5 and AQI in 2020 by Month ---")
    spark.sql("""
        SELECT 
            year,
            month,
            ROUND(AVG(PM2_5), 2) AS avg_pm25,
            ROUND(AVG(AQI), 2) AS avg_aqi,
            COUNT(*) AS hourly_readings
        FROM station_hour
        WHERE year = 2020
        GROUP BY year, month
        ORDER BY month ASC
    """).show(12, truncate=False)

    print("\n--- 2. Top 5 Most Polluted Stations by Severe AQI Count in 2019-2020 ---")
    spark.sql("""
        SELECT 
            StationId,
            COUNT(*) AS severe_hours_count,
            ROUND(AVG(AQI), 1) AS avg_aqi
        FROM station_hour
        WHERE year >= 2019 AND AQI_Bucket = 'Severe'
        GROUP BY StationId
        ORDER BY severe_hours_count DESC
        LIMIT 5
    """).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
