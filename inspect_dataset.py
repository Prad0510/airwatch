"""
inspect_dataset.py
------------------
Script to inspect the Indian Air Quality station_hour.csv dataset:
1. Columns and Dimensions
2. Data Types and Memory Usage
3. Missing Values and Null Percentages
4. Duplicate Rows (exact and primary key based)
5. Statistical and Temporal summary
6. Generates recommendations for HDFS storage and partitioning
"""

import os
import json
import time
import pandas as pd
import numpy as np

def run_inspection(filepath="data/station_hour.csv", output_report="inspection_report.json"):
    print(f"=== Starting inspection of {filepath} ===")
    start_time = time.time()
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Dataset not found at {filepath}")

    file_size_bytes = os.path.getsize(filepath)
    file_size_mb = file_size_bytes / (1024 * 1024)
    print(f"File size on disk: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")

    # Read dataset
    # We will read with pandas; let's parse Datetime as well to inspect temporal characteristics
    print("Loading CSV into DataFrame...")
    df = pd.read_csv(filepath, low_memory=False)
    load_time = time.time() - start_time
    print(f"Data loaded in {load_time:.2f} seconds.")

    rows, cols = df.shape
    memory_usage_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

    print("\n--- 1. Dimensions & Basic Info ---")
    print(f"Total Rows: {rows:,}")
    print(f"Total Columns: {cols}")
    print(f"In-Memory Size (unoptimized): {memory_usage_mb:.2f} MB")
    print(f"Columns: {list(df.columns)}")

    print("\n--- 2. Data Types ---")
    dtypes_dict = {col: str(dtype) for col, dtype in df.dtypes.items()}
    for col, dt in dtypes_dict.items():
        print(f"  {col:<15} : {dt}")

    print("\n--- 3. Missing Values Analysis ---")
    null_counts = df.isnull().sum()
    null_pct = (null_counts / rows) * 100
    missing_df = pd.DataFrame({
        "MissingCount": null_counts,
        "MissingPercentage": null_pct.round(2)
    }).sort_values(by="MissingPercentage", ascending=False)
    print(missing_df.to_string())

    print("\n--- 4. Duplicates Analysis ---")
    exact_duplicates = int(df.duplicated().sum())
    print(f"Exact Duplicate Rows across all columns: {exact_duplicates:,}")

    # Check key combinations (StationId, Datetime)
    has_keys = 'StationId' in df.columns and 'Datetime' in df.columns
    key_duplicates = 0
    if has_keys:
        key_duplicates = int(df.duplicated(subset=['StationId', 'Datetime']).sum())
        print(f"Duplicate records on composite key [StationId, Datetime]: {key_duplicates:,}")

    print("\n--- 5. Temporal and Categorical Overview ---")
    temporal_info = {}
    if 'Datetime' in df.columns:
        # Convert Datetime to check min and max
        parsed_dates = pd.to_datetime(df['Datetime'], errors='coerce')
        min_date = str(parsed_dates.min())
        max_date = str(parsed_dates.max())
        total_unique_dates = int(parsed_dates.dt.date.nunique())
        temporal_info = {
            "min_datetime": min_date,
            "max_datetime": max_date,
            "unique_calendar_days": total_unique_dates
        }
        print(f"Date Range: {min_date} to {max_date} ({total_unique_dates:,} unique days)")

    station_info = {}
    if 'StationId' in df.columns:
        unique_stations = int(df['StationId'].nunique())
        station_info = {
            "unique_stations_count": unique_stations,
            "sample_stations": list(df['StationId'].dropna().unique()[:10])
        }
        print(f"Unique Station IDs: {unique_stations}")

    aqi_bucket_info = {}
    if 'AQI_Bucket' in df.columns:
        aqi_bucket_info = df['AQI_Bucket'].value_counts(dropna=False).to_dict()
        print("\nAQI Bucket distribution:")
        for k, v in aqi_bucket_info.items():
            print(f"  {str(k):<15} : {v:,}")

    # Numeric summary
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_summary = {}
    for col in numeric_cols:
        numeric_summary[col] = {
            "min": float(df[col].min()) if not np.isnan(df[col].min()) else None,
            "max": float(df[col].max()) if not np.isnan(df[col].max()) else None,
            "mean": float(df[col].mean()) if not np.isnan(df[col].mean()) else None,
            "median": float(df[col].median()) if not np.isnan(df[col].median()) else None,
        }

    # HDFS Storage Decision Analysis
    hdfs_strategy = {
        "file_format": {
            "recommended": "Apache Parquet (Snappy compression)",
            "alternative": "Apache ORC (ZSTD compression)",
            "rationale": [
                "Parquet uses columnar storage, drastically reducing I/O for analytical queries (e.g., querying only PM2.5 or AQI across stations).",
                "Snappy provides balanced high decompression speed and ~75-80% compression ratio compared to raw CSV (~40-50MB vs 220MB).",
                "Wide ecosystem compatibility with PySpark, Hive, Trino/Presto, Impala, and DuckDB."
            ]
        },
        "partitioning_strategy": {
            "recommended_partition_keys": ["year", "month"],
            "avoid_partitioning_by": ["hour", "date", "station_id"],
            "rationale": [
                "Total records (~2.59M) across ~5 years is medium-sized for Big Data systems.",
                "Partitioning by (year, month) yields ~60-70 partition directories, each containing several megabytes of Parquet data, which avoids the HDFS 'Small Files Problem' that strains NameNode memory.",
                "Partitioning by station_id (~110 stations) or daily/hourly would result in tens of thousands of tiny (<100KB) files, degrading HDFS Block allocation and MapReduce/Spark scheduling.",
                "Queries typically analyze seasonal, monthly, or yearly trends, making (year, month) the ideal partition pruning granularity."
            ]
        },
        "schema_optimization": {
            "StationId": "StringType (or low-cardinality Dictionary)",
            "Datetime": "TimestampType (UTC normalized)",
            "Pollutants (PM2.5, PM10, NO, NO2, NOx, NH3, CO, SO2, O3, Benzene, Toluene, Xylene)": "FloatType (Float32 is sufficient, saving 50% vs DoubleType Float64)",
            "AQI": "FloatType / IntegerType",
            "AQI_Bucket": "StringType / Categorical"
        },
        "target_hdfs_path": "/data/air_quality/raw/station_hour/ (landing) and /data/air_quality/curated/station_hour/year=YYYY/month=MM/ (parquet)"
    }

    report = {
        "metadata": {
            "source_file": filepath,
            "file_size_bytes": file_size_bytes,
            "file_size_mb": round(file_size_mb, 2),
            "rows": rows,
            "columns_count": cols,
            "columns": list(df.columns),
            "in_memory_mb": round(memory_usage_mb, 2),
            "inspection_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        },
        "dtypes": dtypes_dict,
        "missing_values": missing_df.to_dict(orient="index"),
        "duplicates": {
            "exact_duplicate_rows": exact_duplicates,
            "composite_key_duplicates_station_datetime": key_duplicates
        },
        "temporal_info": temporal_info,
        "station_info": station_info,
        "aqi_bucket_distribution": aqi_bucket_info,
        "hdfs_storage_strategy": hdfs_strategy
    }

    with open(output_report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"\n[SUCCESS] Inspection report saved to {output_report}")
    return report

if __name__ == "__main__":
    run_inspection()
