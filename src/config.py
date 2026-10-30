"""
src/config.py
-------------
Configuration, schema definitions, and paths for AirWatch data ingestion.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, FloatType
)

# Raw CSV column mapping (handling special characters like dot in PM2.5)
RAW_COLUMNS = [
    "StationId", "Datetime", "PM2.5", "PM10", "NO", "NO2", "NOx",
    "NH3", "CO", "SO2", "O3", "Benzene", "Toluene", "Xylene",
    "AQI", "AQI_Bucket"
]

# Strict Target Schema for Curated Layer
# Renaming 'PM2.5' to 'PM2_5' is essential for Hive/Parquet compatibility
CURATED_SCHEMA = StructType([
    StructField("StationId", StringType(), False),
    StructField("Datetime", TimestampType(), False),
    StructField("PM2_5", FloatType(), True),
    StructField("PM10", FloatType(), True),
    StructField("NO", FloatType(), True),
    StructField("NO2", FloatType(), True),
    StructField("NOx", FloatType(), True),
    StructField("NH3", FloatType(), True),
    StructField("CO", FloatType(), True),
    StructField("SO2", FloatType(), True),
    StructField("O3", FloatType(), True),
    StructField("Benzene", FloatType(), True),
    StructField("Toluene", FloatType(), True),
    StructField("Xylene", FloatType(), True),
    StructField("AQI", FloatType(), True),
    StructField("AQI_Bucket", StringType(), True)
])

# Pollutant columns for numeric cleaning & bounds validation
POLLUTANT_COLUMNS = [
    "PM2_5", "PM10", "NO", "NO2", "NOx", "NH3",
    "CO", "SO2", "O3", "Benzene", "Toluene", "Xylene", "AQI"
]

# Default HDFS and Local Paths
DEFAULT_HDFS_RAW_INPUT = "hdfs://namenode:9000/data/air_quality/raw/station_hour/station_hour.csv"
DEFAULT_HDFS_CURATED_OUTPUT = "hdfs://namenode:9000/data/air_quality/curated/station_hour/"

DEFAULT_LOCAL_RAW_INPUT = "data/station_hour.csv"
DEFAULT_LOCAL_CURATED_OUTPUT = "data/station_hour_curated_parquet"

DEFAULT_PARTITION_COLS = ["year", "month"]
