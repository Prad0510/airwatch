"""
convert_to_hdfs_parquet.py
--------------------------
Converts the raw station_hour.csv dataset into an optimized, partitioned
Parquet dataset formatted for HDFS and analytical engines (Spark, Hive, Trino/Presto).

Transformations:
1. Handling missing values & sensor anomalies (< 0 converted to NaN/null)
2. Deduplication on (StationId, Datetime)
3. Converting Datetime into proper timestamp
4. Deriving year, month, day, hour, day_of_week
5. Deriving CPCB pollutant classifications (PM2_5_Class, PM10_Class)
6. Enriching with Station metadata (City, State) from data/stations.csv
7. Saving Snappy-compressed Parquet partitioned by year and month
"""

import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

def classify_cpcb_pm25(val):
    if pd.isna(val):
        return None
    if val <= 30:
        return "Good"
    elif val <= 60:
        return "Satisfactory"
    elif val <= 90:
        return "Moderate"
    elif val <= 120:
        return "Poor"
    elif val <= 250:
        return "Very Poor"
    else:
        return "Severe"

def classify_cpcb_pm10(val):
    if pd.isna(val):
        return None
    if val <= 50:
        return "Good"
    elif val <= 100:
        return "Satisfactory"
    elif val <= 250:
        return "Moderate"
    elif val <= 350:
        return "Poor"
    elif val <= 430:
        return "Very Poor"
    else:
        return "Severe"

def convert_and_partition(
    input_csv="data/station_hour.csv",
    stations_csv="data/stations.csv",
    output_dir="data/station_hour_parquet"
):
    print(f"Reading {input_csv}...")
    
    # Define optimal dtypes for memory & HDFS storage efficiency
    float_cols = ['PM2.5', 'PM10', 'NO', 'NO2', 'NOx', 'NH3', 'CO', 'SO2', 'O3', 'Benzene', 'Toluene', 'Xylene', 'AQI']
    dtype_dict = {col: 'float32' for col in float_cols}
    dtype_dict['StationId'] = 'str'
    dtype_dict['AQI_Bucket'] = 'str'

    df = pd.read_csv(input_csv, dtype=dtype_dict)
    
    # 1. Rename 'PM2.5' to 'PM2_5' for Hive/Parquet compatibility
    if 'PM2.5' in df.columns:
        df.rename(columns={'PM2.5': 'PM2_5'}, inplace=True)
    
    # 2. String trimming & drop missing keys
    df['StationId'] = df['StationId'].str.strip()
    df.dropna(subset=['StationId', 'Datetime'], inplace=True)

    # 3. Deduplicate on composite primary key
    initial_len = len(df)
    df.drop_duplicates(subset=['StationId', 'Datetime'], inplace=True)
    print(f"Deduplication complete: {initial_len - len(df)} duplicate records dropped.")

    # 4. Clean sensor anomalies (< 0 replaced with NaN)
    pollutant_cols = ['PM2_5', 'PM10', 'NO', 'NO2', 'NOx', 'NH3', 'CO', 'SO2', 'O3', 'Benzene', 'Toluene', 'Xylene', 'AQI']
    for c in pollutant_cols:
        if c in df.columns:
            df.loc[df[c] < 0, c] = np.nan

    # 5. Timestamp and partition derivations
    print("Parsing timestamps and deriving partition columns...")
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df['year'] = df['Datetime'].dt.year.astype('int16')
    df['month'] = df['Datetime'].dt.month.astype('int8')
    df['day'] = df['Datetime'].dt.day.astype('int8')
    df['hour'] = df['Datetime'].dt.hour.astype('int8')
    df['day_of_week'] = df['Datetime'].dt.dayofweek.astype('int8')

    # 6. Add CPCB Pollutant Classification columns
    print("Applying CPCB pollutant classifications...")
    df['PM2_5_Class'] = df['PM2_5'].apply(classify_cpcb_pm25)
    df['PM10_Class'] = df['PM10'].apply(classify_cpcb_pm10)

    # 7. Enrich with station metadata (City, State)
    if os.path.exists(stations_csv):
        print(f"Enriching with station metadata from {stations_csv}...")
        st_df = pd.read_csv(stations_csv)
        st_df['StationId'] = st_df['StationId'].str.strip()
        df = df.merge(
            st_df[['StationId', 'City', 'State', 'StationName']],
            on='StationId',
            how='left'
        )

    print(f"Writing partitioned Parquet dataset to: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    # Convert to PyArrow table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Write partitioned dataset using Snappy compression
    pq.write_to_dataset(
        table,
        root_path=output_dir,
        partition_cols=['year', 'month'],
        compression='snappy',
        use_dictionary=True
    )

    total_size = sum(
        os.path.getsize(os.path.join(dirpath, f))
        for dirpath, _, filenames in os.walk(output_dir)
        for f in filenames if f.endswith('.parquet')
    )
    
    print(f"[SUCCESS] Conversion complete!")
    print(f"Total Rows Ingested: {len(df):,}")
    print(f"Partitioned Parquet size: {total_size / (1024 * 1024):.2f} MB")
    print(f"Original CSV size: {os.path.getsize(input_csv) / (1024 * 1024):.2f} MB")

    # 8. Auto-export dashboard data for the AirWatch Web UI
    try:
        from export_dashboard_data import export_with_duckdb
        print("[Dashboard] Generating updated dashboard_data.json for UI...")
        export_with_duckdb(output_dir, "inspection_report.json", "dashboard_data.json")
    except Exception as e:
        print(f"[Dashboard] Note: Could not auto-export dashboard data: {e}")

if __name__ == "__main__":
    convert_and_partition()
