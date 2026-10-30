"""
run_analytics_local.py
----------------------
Executes the 4 required analytical SQL queries directly on the partitioned
Parquet dataset (fastest local executor, ANSI SQL compatible).
"""

import duckdb

def main(parquet_path="data/station_hour_parquet"):
    conn = duckdb.connect()
    print("=" * 80)
    print(" AIRWATCH ANALYTICS: DISTRIBUTED SQL QUERY RESULTS")
    print("=" * 80)

    # Register view on Hive partitioned Parquet
    conn.execute(f"""
        CREATE VIEW air_quality AS 
        SELECT * FROM read_parquet('{parquet_path}/**/*.parquet', hive_partitioning=true);
    """)

    print("\n--- 1. AVERAGE PM2.5 AND AQI BY CITY (Top 10 Cities) ---")
    df1 = conn.execute("""
        SELECT 
            COALESCE(City, 'Unknown') AS City,
            COUNT(DISTINCT StationId) AS ActiveStations,
            COUNT(*) AS HourlyReadings,
            ROUND(AVG(PM2_5), 2) AS Avg_PM25,
            ROUND(AVG(PM10), 2) AS Avg_PM10,
            ROUND(AVG(AQI), 2) AS Avg_AQI
        FROM air_quality
        WHERE City IS NOT NULL
        GROUP BY City
        HAVING COUNT(*) > 5000
        ORDER BY Avg_PM25 DESC
        LIMIT 10;
    """).df()
    print(df1.to_string(index=False))

    print("\n--- 2. TOP 10 MOST POLLUTED MONITORING STATIONS ---")
    df2 = conn.execute("""
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
        LIMIT 10;
    """).df()
    print(df2.to_string(index=False))

    print("\n--- 3. MONTHLY POLLUTION TRENDS (Winter Spikes: 2018 - 2020) ---")
    df3 = conn.execute("""
        SELECT 
            month,
            ROUND(AVG(CASE WHEN year = 2018 THEN PM2_5 END), 2) AS PM25_2018,
            ROUND(AVG(CASE WHEN year = 2019 THEN PM2_5 END), 2) AS PM25_2019,
            ROUND(AVG(CASE WHEN year = 2020 THEN PM2_5 END), 2) AS PM25_2020,
            ROUND(AVG(PM2_5), 2) AS Overall_Monthly_Avg_PM25
        FROM air_quality
        WHERE year >= 2018
        GROUP BY month
        ORDER BY month ASC;
    """).df()
    print(df3.to_string(index=False))

    print("\n--- 4. POLLUTANT CONCENTRATION COMPARISONS & RATIOS (CPCB Categories) ---")
    df4 = conn.execute("""
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
        ORDER BY Avg_PM25 ASC;
    """).df()
    print(df4.to_string(index=False))

if __name__ == "__main__":
    main()
