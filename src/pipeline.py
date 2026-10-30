"""
src/pipeline.py
---------------
Core PySpark ingestion, transformation, and partitioning pipeline.

Transformations:
1. Handling missing values & sensor anomalies (< 0 replaced with NULL)
2. Handling duplicates on composite key (StationId, Datetime)
3. Converting Datetime into proper TimestampType
4. Deriving year, month, day, and hour columns
5. Deriving CPCB pollutant classification columns (PM2_5_Class, PM10_Class)
6. Enriching with Station metadata (City, State) from stations.csv
7. Saving into Snappy-compressed partitioned Parquet format
"""

import os
import time
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, when, year, month, dayofmonth, hour, dayofweek, to_timestamp
)
from src.config import (
    POLLUTANT_COLUMNS, DEFAULT_PARTITION_COLS
)
from src.quality_checks import compute_data_quality_metrics

def get_spark_session(app_name: str = "AirWatch-Ingestion", is_local: bool = False) -> SparkSession:
    """
    Initializes and configures the SparkSession with optimized settings.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    if is_local:
        builder = builder.master("local[*]")
    
    return builder.getOrCreate()

def classify_cpcb_pollutants(df: DataFrame) -> DataFrame:
    """
    Adds CPCB (Central Pollution Control Board) classification categories
    for PM2.5 and PM10.
    """
    # PM2.5 standard categories (ug/m3)
    pm25_class = (
        when(col("PM2_5").isNull(), None)
        .when(col("PM2_5") <= 30, "Good")
        .when((col("PM2_5") > 30) & (col("PM2_5") <= 60), "Satisfactory")
        .when((col("PM2_5") > 60) & (col("PM2_5") <= 90), "Moderate")
        .when((col("PM2_5") > 90) & (col("PM2_5") <= 120), "Poor")
        .when((col("PM2_5") > 120) & (col("PM2_5") <= 250), "Very Poor")
        .otherwise("Severe")
    )

    # PM10 standard categories (ug/m3)
    pm10_class = (
        when(col("PM10").isNull(), None)
        .when(col("PM10") <= 50, "Good")
        .when((col("PM10") > 50) & (col("PM10") <= 100), "Satisfactory")
        .when((col("PM10") > 100) & (col("PM10") <= 250), "Moderate")
        .when((col("PM10") > 250) & (col("PM10") <= 350), "Poor")
        .when((col("PM10") > 350) & (col("PM10") <= 430), "Very Poor")
        .otherwise("Severe")
    )

    return df.withColumn("PM2_5_Class", pm25_class).withColumn("PM10_Class", pm10_class)

def read_and_clean_dataset(
    spark: SparkSession,
    input_path: str,
    stations_path: str = None
) -> DataFrame:
    """
    Reads the raw CSV, normalizes column names, cleans strings,
    handles sensor anomalies (replaces negative values with null),
    deduplicates composite keys, derives temporal partition columns,
    and applies pollutant classifications.
    """
    print(f"[Pipeline] Reading raw data from: {input_path}")
    
    raw_df = spark.read \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .csv(input_path)

    # 1. Rename column 'PM2.5' to 'PM2_5' for Parquet & Hive compatibility
    df = raw_df
    if "PM2.5" in df.columns:
        df = df.withColumnRenamed("PM2.5", "PM2_5")

    # 2. String trimming & Key validations
    df = df.withColumn("StationId", trim(col("StationId")))
    if "AQI_Bucket" in df.columns:
        df = df.withColumn("AQI_Bucket", trim(col("AQI_Bucket")))

    # Drop rows where essential primary keys are null
    df = df.filter(col("StationId").isNotNull() & col("Datetime").isNotNull())

    # 3. Parse timestamp
    df = df.withColumn("Datetime", to_timestamp(col("Datetime"), "yyyy-MM-dd HH:mm:ss"))

    # 4. Handle duplicates on primary composite key (StationId, Datetime)
    df = df.dropDuplicates(["StationId", "Datetime"])

    # 5. Cast numeric pollutant columns to FloatType and replace negative values (< 0) with NULL
    for c in POLLUTANT_COLUMNS:
        if c in df.columns:
            cast_col = col(c).cast("float")
            # Replace physical anomalies (e.g. sensor drift or error code -999) with NULL
            df = df.withColumn(c, when(cast_col < 0, None).otherwise(cast_col))

    # 6. Temporal Feature Engineering & Partition Keys
    df_transformed = df \
        .withColumn("year", year(col("Datetime")).cast("smallint")) \
        .withColumn("month", month(col("Datetime")).cast("smallint")) \
        .withColumn("day", dayofmonth(col("Datetime")).cast("smallint")) \
        .withColumn("hour", hour(col("Datetime")).cast("smallint")) \
        .withColumn("day_of_week", dayofweek(col("Datetime")).cast("smallint"))

    # 7. Add CPCB Pollutant Classification columns
    df_transformed = classify_cpcb_pollutants(df_transformed)

    # 8. Enrich with City/State from stations.csv if provided
    if stations_path and os.path.exists(stations_path):
        print(f"[Pipeline] Enriching with station metadata from: {stations_path}")
        stations_df = spark.read.option("header", "true").csv(stations_path) \
            .select(
                trim(col("StationId")).alias("meta_StationId"),
                trim(col("City")).alias("City"),
                trim(col("State")).alias("State"),
                trim(col("StationName")).alias("StationName")
            )
        df_transformed = df_transformed.join(
            stations_df,
            df_transformed.StationId == stations_df.meta_StationId,
            how="left"
        ).drop("meta_StationId")

    return df_transformed

def write_curated_layer(
    df: DataFrame,
    output_path: str,
    partition_cols: list = None,
    save_mode: str = "overwrite"
):
    """
    Writes the transformed DataFrame into Snappy-compressed partitioned Parquet format.
    """
    if partition_cols is None:
        partition_cols = DEFAULT_PARTITION_COLS

    print(f"[Pipeline] Writing curated Parquet dataset partitioned by {partition_cols} to: {output_path}")
    
    df.write \
        .mode(save_mode) \
        .partitionBy(*partition_cols) \
        .parquet(output_path)

    print("[Pipeline] Data write completed successfully.")

def run_ingestion_pipeline(
    input_path: str,
    output_path: str,
    stations_path: str = None,
    report_output_path: str = None,
    is_local: bool = False
) -> dict:
    """
    Full pipeline orchestration.
    """
    start_time = time.time()
    spark = get_spark_session(is_local=is_local)

    try:
        # Step 1: Ingest, clean, classify, and enrich
        cleaned_df = read_and_clean_dataset(spark, input_path, stations_path=stations_path)

        # Step 2: Quality checks & audit metrics
        metrics = compute_data_quality_metrics(cleaned_df, POLLUTANT_COLUMNS)
        elapsed_sec = round(time.time() - start_time, 2)
        metrics["execution_time_seconds"] = elapsed_sec

        print(f"[Pipeline] Processed {metrics['total_records']:,} valid records in {elapsed_sec}s.")
        print(f"[Pipeline] Duplicate keys detected: {metrics['duplicate_composite_keys']}")

        # Step 3: Write to Curated Parquet
        write_curated_layer(cleaned_df, output_path)

        # Step 4: Write audit report
        if report_output_path:
            with open(report_output_path, "w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=2)
            print(f"[Pipeline] Ingestion report saved to: {report_output_path}")

        return metrics

    finally:
        spark.stop()
