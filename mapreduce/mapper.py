#!/usr/bin/env python3
"""
mapreduce/mapper.py
-------------------
Hadoop Streaming Mapper:
Extracts (StationId, PM2.5) from CSV lines and emits:
  StationId \t PM2.5 \t 1
"""

import sys

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split(",")
        # Skip header row
        if parts[0] == "StationId" or len(parts) < 3:
            continue
        
        station_id = parts[0].strip()
        pm25_str = parts[2].strip() if len(parts) > 2 else ""

        # Validate PM2.5 is a valid positive float
        if pm25_str and pm25_str != "":
            try:
                pm25 = float(pm25_str)
                if pm25 >= 0:
                    # Emit: station_id \t pm25 \t count
                    print(f"{station_id}\t{pm25}\t1")
            except ValueError:
                continue

if __name__ == "__main__":
    main()
