#!/usr/bin/env bash
# ============================================================
# hdfs_verification.sh
# HDFS Distributed Storage Verification & Health Inspection
# ============================================================

echo "============================================================"
echo " 1. HDFS FILE & DIRECTORY EXISTENCE VERIFICATION"
echo "============================================================"
echo "--- Checking Raw Landing Layer ---"
hdfs dfs -ls -h /data/air_quality/raw/station_hour/

echo -e "\n--- Checking Curated Partitioned Parquet Layer ---"
hdfs dfs -ls -h /data/air_quality/curated/station_hour/ | head -n 15

echo -e "\n============================================================"
echo " 2. BLOCK SIZE, REPLICATION & METADATA INSPECTION"
echo "============================================================"
RAW_FILE="/data/air_quality/raw/station_hour/station_hour.csv"
hdfs dfs -stat "File: %n | Block Size: %o bytes (%[o/1048576] MB) | Replication: %r | Size: %b bytes" $RAW_FILE

echo -e "\n============================================================"
echo " 3. HDFS FSCK (FILESYSTEM CHECK - BLOCKS & LOCATIONS)"
echo "============================================================"
# Inspect block distribution and healthy/under-replicated blocks across DataNodes
hdfs fsck /data/air_quality/ -files -blocks -locations

echo -e "\n============================================================"
echo " 4. NAMENODE & DATANODE CLUSTER REPORT"
echo "============================================================"
# Displays NameNode metadata status, active DataNodes, and configured storage capacity
hdfs dfsadmin -report

echo -e "\n============================================================"
echo " 5. TOTAL DISK USAGE BY LAYER"
echo "============================================================"
hdfs dfs -du -h /data/air_quality/
