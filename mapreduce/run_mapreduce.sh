#!/usr/bin/env bash
# ============================================================
# run_mapreduce.sh
# Execute Hadoop Streaming MapReduce job on Indian Air Quality
# ============================================================

INPUT_HDFS="/data/air_quality/raw/station_hour/station_hour.csv"
OUTPUT_HDFS="/data/air_quality/analytics/mr_station_pm25"

# Remove existing output directory if it exists
hdfs dfs -rm -r -f $OUTPUT_HDFS

# Find streaming jar dynamically
STREAMING_JAR=$(find / -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1)
if [ -z "$STREAMING_JAR" ]; then
    STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar"
fi

echo "Running Hadoop Streaming Job..."
hadoop jar $STREAMING_JAR \
    -files /workspace/mapreduce/mapper.py,/workspace/mapreduce/reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input $INPUT_HDFS \
    -output $OUTPUT_HDFS

echo "MapReduce job finished. Viewing results:"
hdfs dfs -cat $OUTPUT_HDFS/part-* | head -n 20
