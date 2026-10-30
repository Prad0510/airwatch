"""
tests/test_dashboard.py
------------------------
Unit and integration tests for AirWatch Dashboard Export and Live Server Endpoints.
"""

import os
import json
import unittest
import threading
import time
import urllib.request
import urllib.parse
from http.server import HTTPServer

from export_dashboard_data import export_with_duckdb
from serve_dashboard import AirWatchDashboardHandler, get_hdfs_and_cluster_status

class TestDashboardExport(unittest.TestCase):
    def setUp(self):
        self.test_output = "tests/test_dashboard_data.json"
        if os.path.exists(self.test_output):
            os.remove(self.test_output)

    def tearDown(self):
        if os.path.exists(self.test_output):
            os.remove(self.test_output)

    def test_duckdb_export_structure(self):
        # Run export over the partitioned parquet dataset
        data = export_with_duckdb(
            parquet_path="data/station_hour_parquet",
            inspection_report_path="inspection_report.json",
            output_path=self.test_output
        )

        self.assertTrue(os.path.exists(self.test_output))
        self.assertFalse(data["sample"])
        self.assertTrue(data["live"])

        # Check meta keys
        self.assertIn("rows", data["meta"])
        self.assertIn("stations", data["meta"])
        self.assertIn("days", data["meta"])

        # Check buckets
        for bucket in ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe", "No AQI"]:
            self.assertIn(bucket, data["buckets"])
            self.assertGreater(data["buckets"][bucket], 0)

        # Check cities (Top 10)
        self.assertGreaterEqual(len(data["cities"]), 5)
        top_city = data["cities"][0]
        self.assertIn("city", top_city)
        self.assertIn("pm25", top_city)
        self.assertIn("stations", top_city)
        self.assertGreater(top_city["pm25"], 0)

        # Check stations (Top 10)
        self.assertGreaterEqual(len(data["stations"]), 5)
        top_station = data["stations"][0]
        self.assertIn("name", top_station)
        self.assertIn("mean", top_station)
        self.assertIn("severe", top_station)

        # Check monthly heatmap
        self.assertIn("2018", data["heat"])
        self.assertEqual(len(data["heat"]["2018"]), 12)

        # Check hourly curve
        self.assertEqual(len(data["hourly"]), 24)
        for h in data["hourly"]:
            self.assertIsInstance(h, (int, float))

        # Check classes
        self.assertEqual(len(data["classes"]), 6)
        class_names = [c["cls"] for c in data["classes"]]
        self.assertIn("Good", class_names)
        self.assertIn("Severe", class_names)

class TestDashboardServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Pick an arbitrary test port
        cls.port = 8899
        cls.server = HTTPServer(("localhost", cls.port), AirWatchDashboardHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever)
        cls.server_thread.daemon = True
        cls.server_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_get_root_html(self):
        url = f"http://localhost:{self.port}/"
        req = urllib.request.urlopen(url)
        self.assertEqual(req.status, 200)
        content = req.read().decode("utf-8")
        self.assertIn("AirWatch: Indian air quality dashboard", content)
        self.assertIn("<canvas id=\"cityChart\">", content)

    def test_get_dashboard_data_api(self):
        url = f"http://localhost:{self.port}/api/dashboard-data"
        req = urllib.request.urlopen(url)
        self.assertEqual(req.status, 200)
        data = json.loads(req.read().decode("utf-8"))
        self.assertIn("cities", data)
        self.assertIn("stations", data)
        self.assertIn("hourly", data)

    def test_post_refresh_api(self):
        url = f"http://localhost:{self.port}/api/refresh"
        req = urllib.request.Request(url, data=b"", method="POST")
        res = urllib.request.urlopen(req)
        self.assertEqual(res.status, 200)
        data = json.loads(res.read().decode("utf-8"))
        self.assertIn("refresh_time_sec", data)
        self.assertTrue(data["live"])

    def test_get_hdfs_status_api(self):
        url = f"http://localhost:{self.port}/api/hdfs-status"
        req = urllib.request.urlopen(url)
        self.assertEqual(req.status, 200)
        status = json.loads(req.read().decode("utf-8"))
        self.assertIn("hdfs_connected", status)
        self.assertIn("local_parquet", status)
        self.assertTrue(status["local_parquet"]["exists"])

    def test_get_benchmark_api(self):
        url = f"http://localhost:{self.port}/api/benchmark"
        req = urllib.request.urlopen(url)
        self.assertEqual(req.status, 200)
        bench = json.loads(req.read().decode("utf-8"))
        self.assertIn("speedup_vs_csv", bench)
        self.assertIn("execution_time_seconds", bench)

if __name__ == "__main__":
    unittest.main()

