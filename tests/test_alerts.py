"""
tests/test_alerts.py
--------------------
Unit tests for the AirWatch Automated Alert Engine.
"""

import os
import json
import unittest
from datetime import datetime, timedelta

from src.alerts import (
    get_alert_level,
    detect_severe_streaks,
    send_slack_webhook,
    send_email_alert,
    save_alerts_audit_log,
    CPCB_SEVERE_PM25_THRESHOLD
)

class TestAlertLogic(unittest.TestCase):
    def test_alert_levels(self):
        self.assertEqual(get_alert_level(6), "WARNING")
        self.assertEqual(get_alert_level(11), "WARNING")
        self.assertEqual(get_alert_level(12), "CRITICAL")
        self.assertEqual(get_alert_level(23), "CRITICAL")
        self.assertEqual(get_alert_level(24), "EMERGENCY")
        self.assertEqual(get_alert_level(48), "EMERGENCY")

    def test_slack_webhook_dry_run(self):
        sample_incident = {
            "incident_id": "ALERT-DL002-1600000000",
            "station_id": "DL002",
            "station_name": "Anand Vihar, Delhi - DPCC",
            "city": "Delhi",
            "streak_start": "2020-01-01 00:00:00",
            "streak_end": "2020-01-01 08:00:00",
            "consecutive_hours": 8,
            "level": "WARNING",
            "avg_pm25": 320.5,
            "peak_pm25": 450.0,
            "peak_aqi": 460.0,
            "message": "CPCB Severe Threshold Exceeded for 8 hours."
        }

        res = send_slack_webhook(sample_incident, webhook_url=None)
        self.assertTrue(res["success"])
        self.assertEqual(res["mode"], "simulated_dry_run")
        self.assertIn("payload", res)
        self.assertIn("attachments", res["payload"])
        attachment = res["payload"]["attachments"][0]
        self.assertIn("Anand Vihar", attachment["fields"][0]["value"])

    def test_email_alert_dry_run(self):
        sample_incident = {
            "incident_id": "ALERT-DL002-1600000000",
            "station_id": "DL002",
            "station_name": "Anand Vihar, Delhi - DPCC",
            "city": "Delhi",
            "streak_start": "2020-01-01 00:00:00",
            "streak_end": "2020-01-01 14:00:00",
            "consecutive_hours": 14,
            "level": "CRITICAL",
            "avg_pm25": 380.0,
            "peak_pm25": 520.0,
            "peak_aqi": 490.0,
            "message": "CPCB Severe Threshold Exceeded for 14 hours."
        }

        res = send_email_alert(sample_incident, to_email="test@airwatch.internal", dry_run=True)
        self.assertTrue(res["success"])
        self.assertEqual(res["mode"], "simulated_email")
        self.assertEqual(res["recipient"], "test@airwatch.internal")
        self.assertIn("CRITICAL", res["subject"])

    def test_save_alerts_audit_log(self):
        test_log = "tests/test_alerts_log.json"
        if os.path.exists(test_log):
            os.remove(test_log)

        sample_incidents = [
            {
                "incident_id": "TEST-1",
                "station_id": "DL001",
                "station_name": "Test Station",
                "city": "Delhi",
                "consecutive_hours": 7,
                "level": "WARNING"
            }
        ]

        save_alerts_audit_log(sample_incidents, output_file=test_log)
        self.assertTrue(os.path.exists(test_log))

        with open(test_log, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertEqual(data["total_incidents"], 1)
            self.assertEqual(data["severe_threshold_pm25"], CPCB_SEVERE_PM25_THRESHOLD)

        if os.path.exists(test_log):
            os.remove(test_log)

    def test_detect_severe_streaks_parquet(self):
        # Scan curated parquet for streaks >= 12 hours (recent year only for fast test)
        incidents = detect_severe_streaks(
            parquet_path="data/station_hour_parquet",
            min_consecutive_hours=12,
            recent_only=True
        )
        self.assertIsInstance(incidents, list)
        if incidents:
            top = incidents[0]
            self.assertGreaterEqual(top["consecutive_hours"], 12)
            self.assertIn(top["level"], ["CRITICAL", "EMERGENCY"])
            self.assertIn("station_id", top)
            self.assertIn("city", top)

if __name__ == "__main__":
    unittest.main()

