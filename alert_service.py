"""
alert_service.py
----------------
AirWatch Automated Alert Service Entrypoint.

Scans the curated Parquet dataset for prolonged Severe pollution episodes (>6 consecutive hours),
dispatches notifications via Slack/Webhook and Email, and records audit logs.

Usage:
    # Scan all records for Severe streaks >= 6 hours (default dry-run)
    python alert_service.py

    # Scan with custom hours threshold
    python alert_service.py --min-hours 8

    # Dispatch to live Slack/Webhook URL
    python alert_service.py --webhook-url https://hooks.slack.com/services/...

    # Filter to most recent year only
    python alert_service.py --recent
"""

import sys
import argparse
from src.alerts import (
    detect_severe_streaks,
    send_slack_webhook,
    send_email_alert,
    save_alerts_audit_log,
    CPCB_SEVERE_PM25_THRESHOLD
)

def main():
    parser = argparse.ArgumentParser(description="AirWatch Automated Severe Air Quality Alert Engine")
    parser.add_argument(
        "--path",
        default="data/station_hour_parquet",
        help="Path to curated Parquet dataset"
    )
    parser.add_argument(
        "--min-hours",
        type=int,
        default=6,
        help="Minimum consecutive hours in Severe category to trigger an alert (default: 6)"
    )
    parser.add_argument(
        "--recent",
        action="store_true",
        help="Scan only the most recent year in the dataset"
    )
    parser.add_argument(
        "--webhook-url",
        default=None,
        help="Slack or generic Webhook URL for alert notifications"
    )
    parser.add_argument(
        "--email",
        default=None,
        help="Recipient email address for alert notification"
    )
    parser.add_argument(
        "--output",
        default="alerts_log.json",
        help="Path for saving alert audit log JSON"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum incidents to display in console output (default: 10)"
    )

    args = parser.parse_args()

    print("=" * 75)
    print(" [ALERT] AIRWATCH AUTOMATED SEVERE AIR QUALITY ALERT ENGINE")
    print("=" * 75)
    print(f" Dataset Path         : {args.path}")
    print(f" Severe Threshold     : PM2.5 > {CPCB_SEVERE_PM25_THRESHOLD} ug/m3 or AQI Bucket = Severe")
    print(f" Consecutive Window   : > {args.min_hours} hours")
    print(f" Filter Mode          : {'Most Recent Year' if args.recent else 'Full Historical Dataset'}")
    print("=" * 75)

    print("\n[Alert Engine] Scanning curated dataset for prolonged severe streaks...")
    incidents = detect_severe_streaks(
        parquet_path=args.path,
        min_consecutive_hours=args.min_hours,
        recent_only=args.recent
    )

    print(f"[Alert Engine] Detected {len(incidents):,} incidents exceeding {args.min_hours} consecutive hours.")

    if not incidents:
        print("[Alert Engine] No stations exceeded the threshold. Air quality compliant.")
        return

    # Display Top Incidents
    print("\n--- TOP DETECTED PROLONGED SEVERE POLLUTION EPISODES ---")
    headers = f"{'Level':<10} {'Station':<28} {'City':<12} {'Hours':<7} {'Peak PM2.5':<12} {'Streak End'}"
    print(headers)
    print("-" * len(headers))
    for inc in incidents[:args.limit]:
        print(
            f"{inc['level']:<10} "
            f"{inc['station_name'][:26]:<28} "
            f"{inc['city']:<12} "
            f"{inc['consecutive_hours']:<7} "
            f"{inc['peak_pm25']:<12.1f} "
            f"{inc['streak_end']}"
        )

    # Dispatch alerts for top incident as a demonstration
    top_incident = incidents[0]
    print("\n[Alert Engine] Dispatching notifications for highest severity incident...")
    
    # 1. Webhook / Slack
    res_webhook = send_slack_webhook(top_incident, webhook_url=args.webhook_url)
    print(f"  -> Slack/Webhook: {res_webhook['mode']} ({'SUCCESS' if res_webhook.get('success') else 'FAILED'})")

    # 2. Email
    res_email = send_email_alert(top_incident, to_email=args.email, dry_run=args.email is None)
    print(f"  -> Email Alert  : {res_email['mode']} to {res_email.get('recipient')} ({'SUCCESS' if res_email.get('success') else 'FAILED'})")

    # Save to persistent audit log
    save_alerts_audit_log(incidents, output_file=args.output)
    print("\n[SUCCESS] Alert engine run complete.")

if __name__ == "__main__":
    main()
