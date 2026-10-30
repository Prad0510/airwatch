"""
src/quality_checks.py
---------------------
Data quality assertions, anomaly detection, and audit metrics.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull

def compute_data_quality_metrics(df: DataFrame, pollutant_cols: list) -> dict:
    """
    Computes a comprehensive summary of record counts, null percentages,
    and out-of-range sensor readings.
    """
    total_records = df.count()
    if total_records == 0:
        return {"error": "Dataset is empty"}

    # Build aggregations for null counts
    null_exprs = [
        count(when(isnull(c) | isnan(c), c)).alias(f"{c}_nulls")
        for c in pollutant_cols
    ]
    null_results = df.select(null_exprs).collect()[0].asDict()

    null_summary = {}
    for c in pollutant_cols:
        count_null = null_results.get(f"{c}_nulls", 0)
        pct = (count_null / total_records) * 100
        null_summary[c] = {
            "null_count": count_null,
            "null_percentage": round(pct, 2)
        }

    # Count negative values (sensor errors) across pollutants
    neg_exprs = [
        count(when(col(c) < 0, c)).alias(f"{c}_negative")
        for c in pollutant_cols
    ]
    neg_results = df.select(neg_exprs).collect()[0].asDict()
    negative_summary = {
        c: neg_results.get(f"{c}_negative", 0)
        for c in pollutant_cols
        if neg_results.get(f"{c}_negative", 0) > 0
    }

    # Check distinct primary keys
    distinct_keys = df.select("StationId", "Datetime").distinct().count()
    duplicate_keys = total_records - distinct_keys

    return {
        "total_records": total_records,
        "duplicate_composite_keys": duplicate_keys,
        "null_summary": null_summary,
        "negative_anomaly_counts": negative_summary
    }
