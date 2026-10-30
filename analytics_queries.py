"""
analytics_queries.py
--------------------
Distributed Analytics using Spark SQL on curated Air Quality dataset.

Analyses Implemented:
1. Average PM2.5 and AQI by City
2. Top 10 Most Polluted Monitoring Stations
3. Monthly Pollution Trends across Years (Seasonal Variation)
4. Pollutant Concentration Comparisons (PM2.5 vs PM10, NOx, CO)
"""

import os
import argparse
from pyspark.sql import SparkSession

def run_distributed_analytics(parquet_path: str):
    spark = SparkSession.builder \
        .appName("AirWatch-Distributed-Analytics") \
        .getOrCreate()

    print(f"\n[Analytics] Loading curated Parquet table from: {parquet_path}")
    df = spark.read.parquet(parquet_path)
    df.createOrReplaceTempView("air_quality")

    print("\n" + "=" * 75)
    print(" 1. AVERAGE PM2.5 AND AQI BY CITY (Top 15 Cities)")
    print("=" * 75)
    spark.sql("""
        SELECT 
            COALESCE(City, 'Unknown') AS City,
            COUNT(DISTINCT StationId) AS ActiveStations,
            COUNT(*) AS TotalHourlyReadings,
            ROUND(AVG(PM2_5), 2) AS Avg_PM25,
            ROUND(AVG(PM10), 2) AS Avg_PM10,
            ROUND(AVG(AQI), 2) AS Avg_AQI
        FROM air_quality
        WHERE City IS NOT NULL
        GROUP BY City
        HAVING COUNT(*) > 5000
        ORDER BY Avg_PM25 DESC
        LIMIT 15
    """).show(15, truncate=False)

    print("\n" + "=" * 75)
    print(" 2. TOP 10 MOST POLLUTED MONITORING STATIONS")
    print("=" * 75)
    spark.sql("""
        SELECT 
            StationId,
            COALESCE(StationName, StationId) AS StationName,
            COALESCE(City, 'Unknown') AS City,
            ROUND(AVG(PM2_5), 1) AS Mean_PM25,
            ROUND(MAX(PM2_5), 1) AS Max_PM25,
            SUM(CASE WHEN AQI_Bucket = 'Severe' THEN 1 ELSE 0 END) AS Severe_AQI_Hours
        FROM air_quality
        GROUP BY StationId, StationName, City
        HAVING COUNT(PM2_5) > 1000
        ORDER BY Mean_PM25 DESC
        LIMIT 10
    """).show(10, truncate=False)

    print("\n" + "=" * 75)
    print(" 3. MONTHLY POLLUTION TRENDS ACROSS YEARS (Seasonal Winter Spikes)")
    print("=" * 75)
    spark.sql("""
        SELECT 
            month,
            ROUND(AVG(CASE WHEN year = 2018 THEN PM2_5 END), 2) AS PM25_2018,
            ROUND(AVG(CASE WHEN year = 2019 THEN PM2_5 END), 2) AS PM25_2019,
            ROUND(AVG(CASE WHEN year = 2020 THEN PM2_5 END), 2) AS PM25_2020,
            ROUND(AVG(PM2_5), 2) AS Overall_Monthly_Avg_PM25
        FROM air_quality
        GROUP BY month
        ORDER BY month ASC
    """).show(12, truncate=False)

    print("\n" + "=" * 75)
    print(" 4. POLLUTANT CONCENTRATION COMPARISONS & RATIOS")
    print("=" * 75)
    spark.sql("""
        SELECT 
            PM2_5_Class,
            COUNT(*) AS TotalReadings,
            ROUND(AVG(PM2_5), 2) AS Avg_PM25,
            ROUND(AVG(PM10), 2) AS Avg_PM10,
            ROUND(AVG(PM2_5) / NULLIF(AVG(PM10), 0), 2) AS PM25_to_PM10_Ratio,
            ROUND(AVG(NO2), 2) AS Avg_NO2,
            ROUND(AVG(CO), 2) AS Avg_CO
        FROM air_quality
        WHERE PM2_5_Class IS NOT NULL
        GROUP BY PM2_5_Class
        ORDER BY Avg_PM25 ASC
    """).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default="data/station_hour_parquet", help="Path to curated parquet table")
    args = parser.parse_args()
    run_distributed_analytics(args.path)
