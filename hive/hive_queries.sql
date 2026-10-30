-- ============================================================
-- hive/hive_queries.sql
-- Analytical HiveQL Queries on AirWatch Curated Parquet Table
-- ============================================================

USE air_quality_db;

-- ------------------------------------------------------------
-- Query 1: Calculate Average PM2.5 and AQI by City
-- ------------------------------------------------------------
SELECT 
    COALESCE(City, 'Unknown') AS City,
    COUNT(DISTINCT StationId) AS TotalStations,
    COUNT(*) AS TotalHourlyReadings,
    ROUND(AVG(PM2_5), 2) AS Avg_PM25,
    ROUND(AVG(PM10), 2) AS Avg_PM10,
    ROUND(AVG(AQI), 2) AS Avg_AQI
FROM curated_station_hour
WHERE City IS NOT NULL
GROUP BY City
HAVING COUNT(*) > 5000
ORDER BY Avg_PM25 DESC
LIMIT 15;

-- ------------------------------------------------------------
-- Query 2: Find the Top 10 Most Polluted Monitoring Stations
-- ------------------------------------------------------------
SELECT 
    StationId,
    COALESCE(StationName, StationId) AS StationName,
    COALESCE(City, 'Unknown') AS City,
    ROUND(AVG(PM2_5), 1) AS Mean_PM25,
    ROUND(MAX(PM2_5), 1) AS Max_PM25,
    SUM(CASE WHEN AQI_Bucket = 'Severe' THEN 1 ELSE 0 END) AS Severe_AQI_Hours
FROM curated_station_hour
GROUP BY StationId, StationName, City
HAVING COUNT(PM2_5) > 1000
ORDER BY Mean_PM25 DESC
LIMIT 10;

-- ------------------------------------------------------------
-- Query 3: Monthly Pollution Trends (Partition Pruned: 2018 - 2020)
-- ------------------------------------------------------------
SELECT 
    month,
    ROUND(AVG(CASE WHEN year = 2018 THEN PM2_5 END), 2) AS PM25_2018,
    ROUND(AVG(CASE WHEN year = 2019 THEN PM2_5 END), 2) AS PM25_2019,
    ROUND(AVG(CASE WHEN year = 2020 THEN PM2_5 END), 2) AS PM25_2020,
    ROUND(AVG(PM2_5), 2) AS Overall_Monthly_Avg_PM25
FROM curated_station_hour
WHERE year >= 2018
GROUP BY month
ORDER BY month ASC;

-- ------------------------------------------------------------
-- Query 4: Compare Pollutant Concentrations by CPCB Category
-- ------------------------------------------------------------
SELECT 
    PM2_5_Class,
    COUNT(*) AS ReadingCount,
    ROUND(AVG(PM2_5), 2) AS Mean_PM25,
    ROUND(AVG(PM10), 2) AS Mean_PM10,
    ROUND(AVG(PM2_5) / NULLIF(AVG(PM10), 0), 2) AS PM25_to_PM10_Ratio,
    ROUND(AVG(NO2), 2) AS Mean_NO2,
    ROUND(AVG(CO), 2) AS Mean_CO
FROM curated_station_hour
WHERE PM2_5_Class IS NOT NULL
GROUP BY PM2_5_Class
ORDER BY Mean_PM25 ASC;
