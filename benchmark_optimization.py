"""
benchmark_optimization.py
-------------------------
Benchmark script measuring and comparing:
1. Storage Footprint: Raw CSV vs Partitioned Parquet
2. Scan & Query Performance: CSV vs Partitioned Parquet
3. Partition Pruning Efficiency (filtering by year/month)
"""

import time
import os
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd

def run_benchmarks(
    csv_path="data/station_hour.csv",
    parquet_path="data/station_hour_parquet"
):
    print("=" * 70)
    print(" AIRWATCH BIG DATA OPTIMIZATION & BENCHMARK SUITE")
    print("=" * 70)

    # 1. Storage Comparison
    csv_size_bytes = os.path.getsize(csv_path) if os.path.exists(csv_path) else 0
    csv_size_mb = csv_size_bytes / (1024 * 1024)

    parquet_size_bytes = sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, files in os.walk(parquet_path)
        for f in files if f.endswith('.parquet')
    )
    parquet_size_mb = parquet_size_bytes / (1024 * 1024)
    storage_reduction_pct = ((csv_size_mb - parquet_size_mb) / csv_size_mb) * 100 if csv_size_mb > 0 else 0

    print("\n--- 1. Storage & Compression Comparison ---")
    print(f"Raw CSV Size            : {csv_size_mb:.2f} MB")
    print(f"Curated Parquet Size    : {parquet_size_mb:.2f} MB")
    print(f"Storage Reduction       : {storage_reduction_pct:.1f}% space saved")

    # 2. Query Benchmark: Filter Year 2020 & Calculate Avg PM2.5 & AQI
    print("\n--- 2. Query Performance Benchmark (Analytical Query on Year 2020) ---")

    # Benchmark A: Scanning Raw CSV
    print("[Benchmark A] Reading and filtering raw CSV...")
    t0 = time.time()
    # Read only needed columns if possible, but CSV still requires full text parsing
    df_csv = pd.read_csv(csv_path, usecols=['Datetime', 'PM2.5', 'AQI'])
    df_csv['year'] = pd.to_datetime(df_csv['Datetime']).dt.year
    filtered_csv = df_csv[df_csv['year'] == 2020]
    avg_pm25_csv = filtered_csv['PM2.5'].mean()
    avg_aqi_csv = filtered_csv['AQI'].mean()
    csv_query_time = time.time() - t0
    print(f"  -> CSV Scan & Query Time: {csv_query_time:.2f} seconds")
    print(f"  -> Records processed    : {len(filtered_csv):,} | Avg PM2.5: {avg_pm25_csv:.2f} | Avg AQI: {avg_aqi_csv:.2f}")

    # Benchmark B: Scanning Partitioned Parquet (With Partition Pruning)
    print("\n[Benchmark B] Reading and filtering Partitioned Parquet (with partition pruning)...")
    t0 = time.time()
    dataset = ds.dataset(parquet_path, format="parquet", partitioning="hive")
    # Partition filter: pushes down year == 2020 directly to directory scanner
    scanner = dataset.scanner(
        columns=['PM2_5', 'AQI'],
        filter=ds.field("year") == 2020
    )
    table = scanner.to_table()
    df_parquet = table.to_pandas()
    avg_pm25_pq = df_parquet['PM2_5'].mean()
    avg_aqi_pq = df_parquet['AQI'].mean()
    parquet_query_time = time.time() - t0
    print(f"  -> Parquet Query Time   : {parquet_query_time:.2f} seconds")
    print(f"  -> Records processed    : {len(df_parquet):,} | Avg PM2.5: {avg_pm25_pq:.2f} | Avg AQI: {avg_aqi_pq:.2f}")

    speedup = csv_query_time / parquet_query_time if parquet_query_time > 0 else 0
    print("\n" + "=" * 70)
    print(f" RESULT: Parquet + Partition Pruning is {speedup:.1f}x FASTER than raw CSV!")
    print("=" * 70)

if __name__ == "__main__":
    run_benchmarks()
