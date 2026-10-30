-- ============================================================
-- hive/hive_setup.sql
-- Create Hive database and External Tables for AirWatch
-- ============================================================

CREATE DATABASE IF NOT EXISTS air_quality_db;
USE air_quality_db;

-- ------------------------------------------------------------
-- 1. Raw External Table (Landing Layer - CSV format)
-- ------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_station_hour (
    StationId STRING,
    Datetime STRING,
    PM2_5 FLOAT,
    PM10 FLOAT,
    NO FLOAT,
    NO2 FLOAT,
    NOx FLOAT,
    NH3 FLOAT,
    CO FLOAT,
    SO2 FLOAT,
    O3 FLOAT,
    Benzene FLOAT,
    Toluene FLOAT,
    Xylene FLOAT,
    AQI FLOAT,
    AQI_Bucket STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///data/air_quality/raw/station_hour/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- ------------------------------------------------------------
-- 2. Stations Metadata External Table
-- ------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS stations (
    StationId STRING,
    StationName STRING,
    City STRING,
    State STRING,
    Status STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION 'hdfs:///data/air_quality/metadata/stations/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- ------------------------------------------------------------
-- 3. Curated External Table (Partitioned Parquet layer)
-- ------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS curated_station_hour (
    StationId STRING,
    Datetime TIMESTAMP,
    PM2_5 FLOAT,
    PM10 FLOAT,
    NO FLOAT,
    NO2 FLOAT,
    NOx FLOAT,
    NH3 FLOAT,
    CO FLOAT,
    SO2 FLOAT,
    O3 FLOAT,
    Benzene FLOAT,
    Toluene FLOAT,
    Xylene FLOAT,
    AQI FLOAT,
    AQI_Bucket STRING,
    day SMALLINT,
    hour SMALLINT,
    day_of_week SMALLINT,
    PM2_5_Class STRING,
    PM10_Class STRING,
    City STRING,
    State STRING,
    StationName STRING
)
PARTITIONED BY (year SMALLINT, month SMALLINT)
STORED AS PARQUET
LOCATION 'hdfs:///data/air_quality/curated/station_hour/';

-- Recover all partitions from HDFS directory structure
MSCK REPAIR TABLE curated_station_hour;
