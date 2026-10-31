"""
src/alerts.py
-------------
Automated Alerting Engine for AirWatch.

Detects consecutive hours where monitoring station readings exceed CPCB Severe thresholds
(PM2.5 > 250 ug/m3 or AQI_Bucket = 'Severe') for > 6 consecutive hours.
Dispatches notifications via Slack / Webhook, Email (SMTP), and structured audit logs.
"""

import os
import sys
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
import urllib.request
import urllib.error

CPCB_SEVERE_PM25_THRESHOLD = 250.0  # ug/m3
DEFAULT_CONSECUTIVE_HOURS_THRESHOLD = 6

def get_alert_level(consecutive_hours: int) -> str:
    if consecutive_hours >= 24:
        return "EMERGENCY"
    elif consecutive_hours >= 12:
        return "CRITICAL"
    else:
        return "WARNING"

def detect_severe_streaks(
    parquet_path: str = "data/station_hour_parquet",
    min_consecutive_hours: int = DEFAULT_CONSECUTIVE_HOURS_THRESHOLD,
    recent_only: bool = False
) -> list:
    """
    Executes a high-performance vectorized window query over the curated Parquet dataset
    to identify all continuous periods where a station exceeded CPCB Severe thresholds
    for at least `min_consecutive_hours`.
    """
    import duckdb

    conn = duckdb.connect()
    norm_path = parquet_path.replace("\\", "/")
    if not norm_path.endswith(".parquet") and not norm_path.endswith("*"):
        parquet_glob = f"{norm_path}/**/*.parquet"
    else:
        parquet_glob = norm_path

    conn.execute(f"""
        CREATE VIEW aq AS 
        SELECT 
            year,
            StationId, 
            CAST(Datetime AS TIMESTAMP) AS Datetime, 
            PM2_5, 
            AQI, 
            AQI_Bucket,
            COALESCE(City, 'Unknown') AS City,
            COALESCE(StationName, StationId) AS StationName,
            CASE WHEN (PM2_5 > {CPCB_SEVERE_PM25_THRESHOLD} OR AQI_Bucket = 'Severe') THEN 1 ELSE 0 END AS is_severe
        FROM read_parquet('{parquet_glob}', hive_partitioning=true);
    """)

    recent_clause = ""
    if recent_only:
        recent_clause = "WHERE year = (SELECT MAX(year) FROM aq)"

    query = f"""
    WITH flagged AS (
        SELECT 
            year,
            StationId,
            StationName,
            City,
            Datetime,
            PM2_5,
            AQI,
            is_severe,
            LAG(Datetime) OVER (PARTITION BY StationId ORDER BY Datetime) AS prev_dt,
            LAG(is_severe) OVER (PARTITION BY StationId ORDER BY Datetime) AS prev_severe
        FROM aq
        {recent_clause}
    ),
    continuous_blocks AS (
        SELECT 
            *,
            CASE 
                WHEN is_severe = 1 AND (prev_severe = 0 OR prev_severe IS NULL OR epoch(Datetime) - epoch(prev_dt) > 3600) THEN 1 
                ELSE 0 
            END AS is_new_streak
        FROM flagged
        WHERE is_severe = 1
    ),
    grouped AS (
        SELECT 
            *,
            SUM(is_new_streak) OVER (PARTITION BY StationId ORDER BY Datetime) AS streak_group
        FROM continuous_blocks
    ),
    summarized AS (
        SELECT 
            StationId AS station_id,
            FIRST(StationName) AS station_name,
            FIRST(City) AS city,
            MIN(Datetime) AS streak_start,
            MAX(Datetime) AS streak_end,
            COUNT(*) AS consecutive_hours,
            ROUND(AVG(PM2_5), 1) AS avg_pm25,
            ROUND(MAX(PM2_5), 1) AS peak_pm25,
            ROUND(MAX(AQI), 0) AS peak_aqi
        FROM grouped
        GROUP BY StationId, streak_group
        HAVING COUNT(*) >= {min_consecutive_hours}
    )
    SELECT * FROM summarized
    ORDER BY streak_end DESC, consecutive_hours DESC;
    """

    df = conn.execute(query).df()
    
    incidents = []
    for row in df.to_dict(orient="records"):
        hours = int(row["consecutive_hours"])
        alert_item = {
            "incident_id": f"ALERT-{row['station_id']}-{int(row['streak_start'].timestamp())}",
            "station_id": row["station_id"],
            "station_name": row["station_name"],
            "city": row["city"],
            "streak_start": row["streak_start"].strftime("%Y-%m-%d %H:%M:%S"),
            "streak_end": row["streak_end"].strftime("%Y-%m-%d %H:%M:%S"),
            "consecutive_hours": hours,
            "level": get_alert_level(hours),
            "avg_pm25": float(row["avg_pm25"]) if row["avg_pm25"] is not None else 0.0,
            "peak_pm25": float(row["peak_pm25"]) if row["peak_pm25"] is not None else 0.0,
            "peak_aqi": float(row["peak_aqi"]) if row["peak_aqi"] is not None else 0.0,
            "message": (
                f"CPCB Severe Threshold Exceeded: {row['station_name']} ({row['city']}) "
                f"recorded {hours} consecutive hours in Severe category. "
                f"Peak PM2.5: {row['peak_pm25']} ug/m3, Peak AQI: {row['peak_aqi']}."
            )
        }
        incidents.append(alert_item)

    return incidents

def send_slack_webhook(incident: dict, webhook_url: str = None) -> dict:
    """
    Sends a formatted alert notification to a Slack-compatible Webhook.
    If no webhook URL is provided, operates in dry-run mode and logs the payload.
    """
    url = webhook_url or os.environ.get("AIRWATCH_WEBHOOK_URL")

    color_map = {
        "WARNING": "#EF8A2A",
        "CRITICAL": "#D9482F",
        "EMERGENCY": "#8E1B2E"
    }
    color = color_map.get(incident.get("level", "WARNING"), "#D9482F")

    payload = {
        "text": f"[AirWatch Alert - {incident['level']}] {incident['station_name']} ({incident['city']})",
        "attachments": [
            {
                "color": color,
                "title": f"CPCB Severe Pollution Streak: {incident['consecutive_hours']} Consecutive Hours",
                "fields": [
                    {"title": "Station", "value": f"{incident['station_name']} ({incident['station_id']})", "short": True},
                    {"title": "City", "value": incident["city"], "short": True},
                    {"title": "Duration", "value": f"{incident['consecutive_hours']} hours", "short": True},
                    {"title": "Alert Level", "value": incident["level"], "short": True},
                    {"title": "Avg PM2.5", "value": f"{incident['avg_pm25']} µg/m³", "short": True},
                    {"title": "Peak PM2.5", "value": f"{incident['peak_pm25']} µg/m³", "short": True},
                    {"title": "Start Time", "value": incident["streak_start"], "short": True},
                    {"title": "End Time", "value": incident["streak_end"], "short": True}
                ],
                "footer": "AirWatch Big Data Alerting Engine",
                "ts": int(datetime.now(timezone.utc).timestamp())
            }
        ]
    }

    if not url:
        return {
            "success": True,
            "mode": "simulated_dry_run",
            "message": "No webhook URL specified. Alert payload generated successfully.",
            "payload": payload
        }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "AirWatch-Alert-Service/1.0"}
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            status = response.status
            return {"success": 200 <= status < 300, "status_code": status, "mode": "live_webhook"}
    except Exception as e:
        return {"success": False, "error": str(e), "mode": "failed_webhook"}

def send_email_alert(incident: dict, to_email: str = None, dry_run: bool = True) -> dict:
    """
    Sends an email alert notification.
    If dry_run is True or SMTP env vars are unset, returns simulated delivery status.
    """
    recipient = to_email or os.environ.get("ALERT_EMAIL_TO", "alerts@airwatch.internal")
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASSWORD")

    subject = f"[{incident['level']}] AirWatch Alert: {incident['consecutive_hours']}h Severe Exposure in {incident['city']}"
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #13232E;">
        <div style="background-color: #8E1B2E; color: #ffffff; padding: 16px 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">[ALERT] AirWatch Severe Pollution Alert</h2>
            <p style="margin: 4px 0 0; opacity: 0.9;">CPCB Severe Threshold Exceeded for &gt; 6 Consecutive Hours</p>
        </div>
        <div style="border: 1px solid #D5DCE1; border-top: none; padding: 20px; border-radius: 0 0 8px 8px;">
            <p><strong>Station:</strong> {incident['station_name']} ({incident['station_id']})</p>
            <p><strong>City:</strong> {incident['city']}</p>
            <p><strong>Duration:</strong> <span style="color: #D9482F; font-weight: bold;">{incident['consecutive_hours']} consecutive hours</span></p>
            <p><strong>Peak PM2.5:</strong> {incident['peak_pm25']} &mu;g/m&sup3; (CPCB Severe limit: 250 &mu;g/m&sup3;)</p>
            <p><strong>Average PM2.5:</strong> {incident['avg_pm25']} &mu;g/m&sup3;</p>
            <p><strong>Time Window:</strong> {incident['streak_start']} to {incident['streak_end']}</p>
            <hr style="border: none; border-top: 1px solid #D5DCE1; margin: 16px 0;">
            <p style="font-size: 13px; color: #5B6B77;">
                <strong>Advisory:</strong> Health warning of emergency conditions. The entire population is likely to be affected. Avoid outdoor physical activity.
            </p>
        </div>
    </body>
    </html>
    """

    if dry_run or not smtp_host:
        return {
            "success": True,
            "mode": "simulated_email",
            "recipient": recipient,
            "subject": subject,
            "message": "Email generated successfully (simulated delivery)."
        }

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_user or "airwatch-alerts@cluster.local"
        msg["To"] = recipient
        msg.attach(MIMEText(incident["message"], "plain"))
        msg.attach(MIMEText(html_content, "html"))

        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls()
            if smtp_user and smtp_pass:
                server.login(smtp_user, smtp_pass)
            server.sendmail(msg["From"], [recipient], msg.as_string())

        return {"success": True, "mode": "smtp_live", "recipient": recipient}
    except Exception as e:
        return {"success": False, "mode": "smtp_error", "error": str(e)}

def save_alerts_audit_log(incidents: list, output_file: str = "alerts_log.json"):
    """
    Saves detected incidents into a persistent audit log.
    """
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_incidents": len(incidents),
            "severe_threshold_pm25": CPCB_SEVERE_PM25_THRESHOLD,
            "min_consecutive_hours": DEFAULT_CONSECUTIVE_HOURS_THRESHOLD,
            "incidents": incidents
        }, f, indent=2)
    print(f"[Alerts] Saved {len(incidents)} alert incidents to: {output_file}")
