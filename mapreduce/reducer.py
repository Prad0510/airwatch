#!/usr/bin/env python3
"""
mapreduce/reducer.py
--------------------
Hadoop Streaming Reducer:
Aggregates PM2.5 readings per station emitted by mapper after shuffle/sort.
Emits:
  StationId \t TotalReadings \t MeanPM2.5 \t MinPM2.5 \t MaxPM2.5
"""

import sys

def main():
    current_station = None
    sum_pm25 = 0.0
    count_readings = 0
    min_pm25 = float('inf')
    max_pm25 = float('-inf')

    # Emit Header
    print("StationId\tTotalReadings\tMeanPM2.5\tMinPM2.5\tMaxPM2.5")

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split("\t")
        if len(parts) < 3:
            continue

        station_id, pm25_str, count_str = parts[0], parts[1], parts[2]

        try:
            pm25 = float(pm25_str)
            count_val = int(count_str)
        except ValueError:
            continue

        if current_station == station_id:
            sum_pm25 += pm25
            count_readings += count_val
            if pm25 < min_pm25:
                min_pm25 = pm25
            if pm25 > max_pm25:
                max_pm25 = pm25
        else:
            if current_station is not None and count_readings > 0:
                mean_pm25 = round(sum_pm25 / count_readings, 2)
                print(f"{current_station}\t{count_readings}\t{mean_pm25}\t{min_pm25}\t{max_pm25}")
            
            current_station = station_id
            sum_pm25 = pm25
            count_readings = count_val
            min_pm25 = pm25
            max_pm25 = pm25

    # Emit the last station
    if current_station is not None and count_readings > 0:
        mean_pm25 = round(sum_pm25 / count_readings, 2)
        print(f"{current_station}\t{count_readings}\t{mean_pm25}\t{min_pm25}\t{max_pm25}")

if __name__ == "__main__":
    main()
