"""
hdfs_spark_ingest.py
--------------------
AirWatch Production Ingestion Pipeline Entrypoint.

Usage:
  # Inside Docker or Spark Cluster:
  spark-submit --master yarn hdfs_spark_ingest.py

  # Standalone with custom paths:
  python hdfs_spark_ingest.py \
    --input data/station_hour.csv \
    --output data/station_hour_curated_parquet \
    --local
"""

import sys
import argparse
from src.config import (
    DEFAULT_HDFS_RAW_INPUT,
    DEFAULT_HDFS_CURATED_OUTPUT,
    DEFAULT_LOCAL_RAW_INPUT,
    DEFAULT_LOCAL_CURATED_OUTPUT
)
from src.pipeline import run_ingestion_pipeline

def parse_args():
    parser = argparse.ArgumentParser(description="AirWatch PySpark Ingestion Pipeline")
    parser.add_argument(
        "--input",
        default=None,
        help="Input CSV file path (HDFS URI or local path)"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output Parquet directory path (HDFS URI or local path)"
    )
    parser.add_argument(
        "--report",
        default="ingestion_audit_report.json",
        help="Path to output data quality and audit report JSON"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run Spark in local mode (useful for local development without cluster)"
    )
    parser.add_argument(
        "--export-dashboard",
        action="store_true",
        default=True,
        help="Export dashboard_data.json for AirWatch web UI upon completion"
    )
    return parser.parse_args()

def main():
    args = parse_args()

    # Determine default paths based on local vs cluster mode
    if args.local:
        input_path = args.input or DEFAULT_LOCAL_RAW_INPUT
        output_path = args.output or DEFAULT_LOCAL_CURATED_OUTPUT
    else:
        input_path = args.input or DEFAULT_HDFS_RAW_INPUT
        output_path = args.output or DEFAULT_HDFS_CURATED_OUTPUT

    print("=" * 60)
    print(" AirWatch Initial PySpark Ingestion Pipeline")
    print("=" * 60)
    print(f" Mode        : {'Local Spark' if args.local else 'Cluster / HDFS'}")
    print(f" Input       : {input_path}")
    print(f" Output      : {output_path}")
    print(f" Audit Report: {args.report}")
    print("=" * 60)

    try:
        metrics = run_ingestion_pipeline(
            input_path=input_path,
            output_path=output_path,
            report_output_path=args.report,
            is_local=args.local
        )
        print("\n[SUCCESS] Pipeline completed successfully!")
        print(f"Total Records Ingested : {metrics['total_records']:,}")
        print(f"Execution Duration     : {metrics['execution_time_seconds']} seconds")

        if args.export_dashboard:
            try:
                print("\n[Dashboard] Exporting updated metrics for AirWatch Dashboard...")
                from export_dashboard_data import export_with_duckdb, export_with_pyspark
                if args.local or not output_path.startswith("hdfs://"):
                    export_with_duckdb(output_path, args.report, "dashboard_data.json")
                else:
                    export_with_pyspark(output_path, args.report, "dashboard_data.json")
            except Exception as ex:
                print(f"[Dashboard] Note: Could not export dashboard: {ex}")
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed with error: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
