"""
tests/test_pipeline.py
----------------------
Unit and integration tests for AirWatch Ingestion Pipeline.
"""

import os
import unittest
import pandas as pd
from src.pipeline import get_spark_session, read_and_clean_dataset
from src.quality_checks import compute_data_quality_metrics
from src.config import POLLUTANT_COLUMNS

class TestPipelineLogic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session(app_name="TestAirWatch", is_local=True)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.sample_csv_path = "tests/sample_station_hour.csv"
        os.makedirs("tests", exist_ok=True)
        
        sample_data = {
            "StationId": ["DL001", "DL001", "MH002"],
            "Datetime": ["2020-01-01 01:00:00", "2020-01-01 02:00:00", "2020-05-15 12:00:00"],
            "PM2.5": [55.4, -999.0, 120.2],  # Negative sensor anomaly
            "PM10": [110.0, 95.0, None],
            "NO": [12.0, 15.0, 20.0],
            "NO2": [25.0, 30.0, 40.0],
            "NOx": [35.0, 42.0, 58.0],
            "NH3": [None, 10.0, 12.0],
            "CO": [0.8, 1.2, 0.5],
            "SO2": [8.0, 9.5, 11.0],
            "O3": [22.0, 25.0, 30.0],
            "Benzene": [1.5, 2.0, None],
            "Toluene": [4.0, 5.0, None],
            "Xylene": [None, None, None],
            "AQI": [150.0, 160.0, 280.0],
            "AQI_Bucket": ["Moderate", "Moderate", "Poor"]
        }
        df = pd.DataFrame(sample_data)
        df.to_csv(self.sample_csv_path, index=False)

    def tearDown(self):
        if os.path.exists(self.sample_csv_path):
            os.remove(self.sample_csv_path)

    def test_clean_and_transform_pipeline(self):
        df_cleaned = read_and_clean_dataset(self.spark, self.sample_csv_path)
        
        # 1. Verify PM2.5 renamed to PM2_5
        self.assertIn("PM2_5", df_cleaned.columns)
        self.assertNotIn("PM2.5", df_cleaned.columns)

        # 2. Verify partition columns were derived
        self.assertIn("year", df_cleaned.columns)
        self.assertIn("month", df_cleaned.columns)
        self.assertIn("day", df_cleaned.columns)
        self.assertIn("hour", df_cleaned.columns)

        # 3. Verify negative anomaly was converted to null
        rows = df_cleaned.collect()
        self.assertEqual(len(rows), 3)
        row_anomaly = [r for r in rows if r["Datetime"].strftime("%H:%M:%S") == "02:00:00"][0]
        self.assertIsNone(row_anomaly["PM2_5"])

        # 4. Verify quality metrics computation
        metrics = compute_data_quality_metrics(df_cleaned, POLLUTANT_COLUMNS)
        self.assertEqual(metrics["total_records"], 3)
        self.assertEqual(metrics["duplicate_composite_keys"], 0)

if __name__ == "__main__":
    unittest.main()
