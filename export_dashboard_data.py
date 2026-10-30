"""
export_dashboard_data.py
-------------------------
Generates `dashboard_data.json` for the AirWatch UI by querying the
curated Parquet dataset (local or HDFS) and merging with inspection metadata.

Can be run standalone with DuckDB (ultra-fast) or with PySpark (cluster/HDFS mode).
"""

import os
import sys
import json
import argparse
import shutil
from datetime import datetime, timezone

def round_val(val, decimals=1):
    if val is None:
        return None
    try:
        f = float(val)
        return round(f, decimals)
    except (ValueError, TypeError):
        return val

def export_with_duckdb(parquet_path: str, inspection_report_path: str, output_path: str):
    import duckdb

    print(f"[Export] Connecting to DuckDB over Parquet dataset: {parquet_path}")
    conn = duckdb.connect()

    # Create view over partitioned Parquet
    norm_path = parquet_path.replace("\\", "/")
    if not norm_path.endswith(".parquet") and not norm_path.endswith("*"):
        parquet_glob = f"{norm_path}/**/*.parquet"
    else:
        parquet_glob = norm_path

    conn.execute(f"""
        CREATE VIEW air_quality AS 
        SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true);
    """)

    # Load baseline inspection metadata if available
    meta = {
        "rows": 2589083,
        "stations": 110,
        "start": "2015-01-01",
        "end": "2020-07-01",
        "days": 2009,
        "csvMb": 209.5
    }
    buckets = {
        "Good": 152113,
        "Satisfactory": 530164,
        "Moderate": 675008,
        "Poor": 239990,
        "Very Poor": 301150,
        "Severe": 120468,
        "No AQI": 570190
    }
    missing = {
        "Xylene": 80.15,
        "NH3": 47.76,
        "PM10": 43.23,
        "Toluene": 40.26,
        "Benzene": 33.28,
        "SO2": 28.69,
        "O3": 28.04,
        "PM2.5": 25.02,
        "AQI": 22.02,
        "NO": 21.39,
        "NO2": 20.43,
        "CO": 19.28,
        "NOx": 18.96
    }

    if os.path.exists(inspection_report_path):
        try:
            with open(inspection_report_path, "r", encoding="utf-8") as f:
                rep = json.load(f)
                if "metadata" in rep:
                    m = rep["metadata"]
                    meta["rows"] = m.get("rows", meta["rows"])
                    meta["csvMb"] = round_val(m.get("file_size_mb", meta["csvMb"]), 1)
                if "temporal_info" in rep:
                    t = rep["temporal_info"]
                    meta["start"] = t.get("min_datetime", meta["start"])[:10]
                    meta["end"] = t.get("max_datetime", meta["end"])[:10]
                    meta["days"] = t.get("unique_calendar_days", meta["days"])
                if "station_info" in rep:
                    meta["stations"] = rep["station_info"].get("unique_stations_count", meta["stations"])
                if "aqi_bucket_distribution" in rep:
                    dist = rep["aqi_bucket_distribution"]
                    buckets = {
                        "Good": int(dist.get("Good", 0)),
                        "Satisfactory": int(dist.get("Satisfactory", 0)),
                        "Moderate": int(dist.get("Moderate", 0)),
                        "Poor": int(dist.get("Poor", 0)),
                        "Very Poor": int(dist.get("Very Poor", 0)),
                        "Severe": int(dist.get("Severe", 0)),
                        "No AQI": int(dist.get("NaN", dist.get("No AQI", 0)))
                    }
                if "missing_values" in rep:
                    missing = {
                        k: round_val(v["MissingPercentage"], 2)
                        for k, v in rep["missing_values"].items()
                        if k not in ("StationId", "Datetime", "AQI_Bucket")
                    }
        except Exception as e:
            print(f"[Warning] Could not parse inspection report: {e}")

    # 1. Cities (Top 10 by average PM2.5)
    print("[Export] Querying City-level aggregations...")
    cities_rows = conn.execute("""
        SELECT 
            COALESCE(City, 'Unknown') AS city,
            CAST(COUNT(DISTINCT StationId) AS INTEGER) AS stations,
            ROUND(AVG(PM2_5), 1) AS pm25,
            ROUND(COALESCE(AVG(PM10), 0), 1) AS pm10,
            ROUND(COALESCE(AVG(AQI), 0), 0) AS aqi
        FROM air_quality
        WHERE City IS NOT NULL
        GROUP BY City
        HAVING COUNT(*) > 5000
        ORDER BY pm25 DESC
        LIMIT 10;
    """).fetchall()
    cities = [
        {
            "city": r[0],
            "stations": int(r[1]),
            "pm25": round_val(r[2], 1),
            "pm10": round_val(r[3], 1),
            "aqi": round_val(r[4], 0)
        }
        for r in cities_rows
    ]

    # 2. Stations (Top 10 worst stations by mean PM2.5)
    print("[Export] Querying Station-level aggregations...")
    stations_rows = conn.execute("""
        SELECT 
            StationId AS id,
            COALESCE(StationName, StationId) AS name,
            COALESCE(City, 'Unknown') AS city,
            ROUND(AVG(PM2_5), 1) AS mean,
            ROUND(MAX(PM2_5), 1) AS max,
            CAST(SUM(CASE WHEN AQI_Bucket = 'Severe' THEN 1 ELSE 0 END) AS INTEGER) AS severe
        FROM air_quality
        GROUP BY StationId, StationName, City
        HAVING COUNT(PM2_5) > 1000
        ORDER BY mean DESC
        LIMIT 10;
    """).fetchall()
    stations = [
        {
            "id": r[0],
            "name": r[1],
            "city": r[2],
            "mean": round_val(r[3], 1),
            "max": round_val(r[4], 1),
            "severe": int(r[5])
        }
        for r in stations_rows
    ]

    # 3. Monthly Heatmap grid (year -> [Jan..Dec])
    print("[Export] Querying Monthly trends per year...")
    monthly_rows = conn.execute("""
        SELECT 
            year,
            month,
            ROUND(AVG(PM2_5), 1) AS avg_pm25
        FROM air_quality
        WHERE PM2_5 IS NOT NULL
        GROUP BY year, month
        ORDER BY year, month;
    """).fetchall()

    heat = {}
    for y, m, val in monthly_rows:
        y_str = str(y)
        if y_str not in heat:
            heat[y_str] = [None] * 12
        if 1 <= m <= 12:
            heat[y_str][m - 1] = round_val(val, 1)

    # 4. Hourly profile (00 to 23 average PM2.5 across all stations)
    print("[Export] Querying Hourly diurnal curves...")
    hourly_rows = conn.execute("""
        SELECT 
            hour,
            ROUND(AVG(PM2_5), 1) AS avg_pm25
        FROM air_quality
        WHERE PM2_5 IS NOT NULL AND hour IS NOT NULL
        GROUP BY hour
        ORDER BY hour ASC;
    """).fetchall()

    hourly = [0.0] * 24
    for h, val in hourly_rows:
        if 0 <= h < 24:
            hourly[h] = round_val(val, 1) if val is not None else 0.0

    # 5. CPCB Pollutant Classes breakdown
    print("[Export] Querying Pollutant breakdown by CPCB Class...")
    classes_rows = conn.execute("""
        SELECT 
            PM2_5_Class AS cls,
            CAST(COUNT(*) AS INTEGER) AS readings,
            ROUND(AVG(PM2_5), 1) AS pm25,
            ROUND(COALESCE(AVG(PM10), 0), 1) AS pm10,
            ROUND(COALESCE(AVG(NO2), 0), 1) AS no2,
            ROUND(COALESCE(AVG(CO), 0), 2) AS co,
            ROUND(AVG(PM2_5) / NULLIF(AVG(PM10), 0), 2) AS ratio
        FROM air_quality
        WHERE PM2_5_Class IS NOT NULL
        GROUP BY PM2_5_Class;
    """).fetchall()

    class_order = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]
    classes_map = {
        r[0]: {
            "cls": r[0],
            "readings": int(r[1]),
            "pm25": round_val(r[2], 1),
            "pm10": round_val(r[3], 1),
            "no2": round_val(r[4], 1),
            "co": round_val(r[5], 2),
            "ratio": round_val(r[6], 2)
        }
        for r in classes_rows
    }
    classes = [classes_map[c] for c in class_order if c in classes_map]

    # Assemble complete dashboard payload
    dashboard_data = {
        "sample": False,
        "live": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_source": "Partitioned Parquet (Snappy)",
        "meta": meta,
        "buckets": buckets,
        "missing": missing,
        "cities": cities,
        "stations": stations,
        "heat": heat,
        "hourly": hourly,
        "classes": classes
    }

    # Write primary output file
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, indent=2)

    # Also sync to web/ directory if different
    web_copy_path = os.path.join("web", "dashboard_data.json")
    if os.path.abspath(output_path) != os.path.abspath(web_copy_path):
        os.makedirs("web", exist_ok=True)
        shutil.copyfile(output_path, web_copy_path)

    print(f"[SUCCESS] Dashboard data written to: {output_path} and {web_copy_path}")
    return dashboard_data

def export_with_pyspark(parquet_path: str, inspection_report_path: str, output_path: str):
    from pyspark.sql import SparkSession

    print(f"[Export-Spark] Starting PySpark for dataset: {parquet_path}")
    spark = SparkSession.builder \
        .appName("AirWatch-Dashboard-Export") \
        .getOrCreate()

    df = spark.read.parquet(parquet_path)
    df.createOrReplaceTempView("air_quality")

    meta = {"rows": 2589083, "stations": 110, "start": "2015-01-01", "end": "2020-07-01", "days": 2009, "csvMb": 209.5}
    buckets = {"Good": 152113, "Satisfactory": 530164, "Moderate": 675008, "Poor": 239990, "Very Poor": 301150, "Severe": 120468, "No AQI": 570190}
    missing = {"Xylene": 80.15, "NH3": 47.76, "PM10": 43.23, "Toluene": 40.26, "Benzene": 33.28, "SO2": 28.69, "O3": 28.04, "PM2.5": 25.02, "AQI": 22.02, "NO": 21.39, "NO2": 20.43, "CO": 19.28, "NOx": 18.96}

    if os.path.exists(inspection_report_path):
        try:
            with open(inspection_report_path, "r", encoding="utf-8") as f:
                rep = json.load(f)
                if "metadata" in rep:
                    meta["rows"] = rep["metadata"].get("rows", meta["rows"])
                    meta["csvMb"] = round_val(rep["metadata"].get("file_size_mb", meta["csvMb"]), 1)
                if "temporal_info" in rep:
                    meta["start"] = rep["temporal_info"].get("min_datetime", meta["start"])[:10]
                    meta["end"] = rep["temporal_info"].get("max_datetime", meta["end"])[:10]
                    meta["days"] = rep["temporal_info"].get("unique_calendar_days", meta["days"])
                if "station_info" in rep:
                    meta["stations"] = rep["station_info"].get("unique_stations_count", meta["stations"])
                if "aqi_bucket_distribution" in rep:
                    dist = rep["aqi_bucket_distribution"]
                    buckets = {
                        "Good": int(dist.get("Good", 0)),
                        "Satisfactory": int(dist.get("Satisfactory", 0)),
                        "Moderate": int(dist.get("Moderate", 0)),
                        "Poor": int(dist.get("Poor", 0)),
                        "Very Poor": int(dist.get("Very Poor", 0)),
                        "Severe": int(dist.get("Severe", 0)),
                        "No AQI": int(dist.get("NaN", dist.get("No AQI", 0)))
                    }
        except Exception as e:
            print(f"[Warning] Could not parse inspection report: {e}")

    # 1. Cities
    cities_rows = spark.sql("""
        SELECT 
            COALESCE(City, 'Unknown') AS city,
            COUNT(DISTINCT StationId) AS stations,
            ROUND(AVG(PM2_5), 1) AS pm25,
            ROUND(COALESCE(AVG(PM10), 0), 1) AS pm10,
            ROUND(COALESCE(AVG(AQI), 0), 0) AS aqi
        FROM air_quality
        WHERE City IS NOT NULL
        GROUP BY City
        HAVING COUNT(*) > 5000
        ORDER BY pm25 DESC
        LIMIT 10
    """).collect()
    cities = [
        {
            "city": r["city"],
            "stations": int(r["stations"]),
            "pm25": round_val(r["pm25"], 1),
            "pm10": round_val(r["pm10"], 1),
            "aqi": round_val(r["aqi"], 0)
        }
        for r in cities_rows
    ]

    # 2. Stations
    stations_rows = spark.sql("""
        SELECT 
            StationId AS id,
            COALESCE(StationName, StationId) AS name,
            COALESCE(City, 'Unknown') AS city,
            ROUND(AVG(PM2_5), 1) AS mean,
            ROUND(MAX(PM2_5), 1) AS max,
            SUM(CASE WHEN AQI_Bucket = 'Severe' THEN 1 ELSE 0 END) AS severe
        FROM air_quality
        GROUP BY StationId, StationName, City
        HAVING COUNT(PM2_5) > 1000
        ORDER BY mean DESC
        LIMIT 10
    """).collect()
    stations = [
        {
            "id": r["id"],
            "name": r["name"],
            "city": r["city"],
            "mean": round_val(r["mean"], 1),
            "max": round_val(r["max"], 1),
            "severe": int(r["severe"])
        }
        for r in stations_rows
    ]

    # 3. Monthly Heatmap
    monthly_rows = spark.sql("""
        SELECT 
            year,
            month,
            ROUND(AVG(PM2_5), 1) AS avg_pm25
        FROM air_quality
        WHERE PM2_5 IS NOT NULL
        GROUP BY year, month
        ORDER BY year, month
    """).collect()

    heat = {}
    for r in monthly_rows:
        y_str = str(r["year"])
        m = r["month"]
        val = r["avg_pm25"]
        if y_str not in heat:
            heat[y_str] = [None] * 12
        if 1 <= m <= 12:
            heat[y_str][m - 1] = round_val(val, 1)

    # 4. Hourly
    hourly_rows = spark.sql("""
        SELECT 
            hour,
            ROUND(AVG(PM2_5), 1) AS avg_pm25
        FROM air_quality
        WHERE PM2_5 IS NOT NULL AND hour IS NOT NULL
        GROUP BY hour
        ORDER BY hour ASC
    """).collect()

    hourly = [0.0] * 24
    for r in hourly_rows:
        h = r["hour"]
        val = r["avg_pm25"]
        if 0 <= h < 24:
            hourly[h] = round_val(val, 1) if val is not None else 0.0

    # 5. Classes
    classes_rows = spark.sql("""
        SELECT 
            PM2_5_Class AS cls,
            COUNT(*) AS readings,
            ROUND(AVG(PM2_5), 1) AS pm25,
            ROUND(COALESCE(AVG(PM10), 0), 1) AS pm10,
            ROUND(COALESCE(AVG(NO2), 0), 1) AS no2,
            ROUND(COALESCE(AVG(CO), 0), 2) AS co,
            ROUND(AVG(PM2_5) / NULLIF(AVG(PM10), 0), 2) AS ratio
        FROM air_quality
        WHERE PM2_5_Class IS NOT NULL
        GROUP BY PM2_5_Class
    """).collect()

    class_order = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]
    classes_dict = {
        r["cls"]: {
            "cls": r["cls"],
            "readings": int(r["readings"]),
            "pm25": round_val(r["pm25"], 1),
            "pm10": round_val(r["pm10"], 1),
            "no2": round_val(r["no2"], 1),
            "co": round_val(r["co"], 2),
            "ratio": round_val(r["ratio"], 2)
        }
        for r in classes_rows
    }
    classes = [classes_dict[c] for c in class_order if c in classes_dict]

    spark.stop()

    dashboard_data = {
        "sample": False,
        "live": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_source": f"HDFS/Spark Parquet ({parquet_path})",
        "meta": meta,
        "buckets": buckets,
        "missing": missing,
        "cities": cities,
        "stations": stations,
        "heat": heat,
        "hourly": hourly,
        "classes": classes
    }

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, indent=2)

    web_copy_path = os.path.join("web", "dashboard_data.json")
    if os.path.abspath(output_path) != os.path.abspath(web_copy_path):
        os.makedirs("web", exist_ok=True)
        shutil.copyfile(output_path, web_copy_path)

    print(f"[SUCCESS] Spark Dashboard data written to: {output_path} and {web_copy_path}")
    return dashboard_data

def main():
    parser = argparse.ArgumentParser(description="Export AirWatch dashboard JSON from Parquet dataset")
    parser.add_argument(
        "--source",
        default="data/station_hour_parquet",
        help="Path to curated Parquet dataset (local directory or HDFS URI)"
    )
    parser.add_argument(
        "--report",
        default="inspection_report.json",
        help="Path to inspection_report.json"
    )
    parser.add_argument(
        "--output",
        default="dashboard_data.json",
        help="Path for output dashboard_data.json"
    )
    parser.add_argument(
        "--engine",
        choices=["duckdb", "spark", "auto"],
        default="auto",
        help="Query engine: duckdb (fast local) or spark (cluster/HDFS)"
    )

    args = parser.parse_args()

    engine = args.engine
    if engine == "auto":
        if args.source.startswith("hdfs://"):
            engine = "spark"
        else:
            try:
                import duckdb
                engine = "duckdb"
            except ImportError:
                engine = "spark"

    print(f"[AirWatch] Exporting dashboard data using engine: {engine}")
    if engine == "duckdb":
        export_with_duckdb(args.source, args.report, args.output)
    else:
        export_with_pyspark(args.source, args.report, args.output)

if __name__ == "__main__":
    main()
