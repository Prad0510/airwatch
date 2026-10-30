"""
AirWatch Big Data Ingestion Package
"""
from src.pipeline import run_ingestion_pipeline, get_spark_session
from src.config import CURATED_SCHEMA, POLLUTANT_COLUMNS

__all__ = ["run_ingestion_pipeline", "get_spark_session", "CURATED_SCHEMA", "POLLUTANT_COLUMNS"]
