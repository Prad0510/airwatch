# AirWatch: Indian Air Quality Big Data Engineering & Analytics Pipeline

Distributed big data engineering and analytics pipeline for the **Indian Air Quality Dataset** (`station_hour.csv`, **2.59 million records**) using **Hadoop HDFS**, **Apache Spark**, **Apache Parquet**, **MapReduce**, and **Hive**.

---

## 👥 Team Quickstart (Setup in 3 Steps)

### Step 1: Clone and Install Dependencies
```bash
git clone <repo-url> airwatch
cd airwatch
pip install -r requirements.txt
```

### Step 2: Download Dataset & Inspect
```bash
# Download dataset (requires kaggle.json configured or place station_hour.csv and stations.csv into data/)
kaggle datasets download -d rohanrao/air-quality-data-in-india -f station_hour.csv -p ./data
kaggle datasets download -d rohanrao/air-quality-data-in-india -f stations.csv -p ./data

# Run inspection & profiling over all 2.59M records
python inspect_dataset.py
```

### Step 3: Run Ingestion, Optimization & Analytics
You can run all components either locally or on the Docker HDFS/Spark cluster:

#### A. Instant Local Execution (No Docker needed)
```bash
# 1. Ingest, clean, classify (CPCB standards), enrich with City metadata, and partition into Parquet
python convert_to_hdfs_parquet.py

# 2. Run Optimization Benchmark (CSV vs Partitioned Parquet: 25.6x speedup)
python benchmark_optimization.py

# 3. Execute all 4 Distributed Analytics Queries (City PM2.5, Top Stations, Monthly Trends, Pollutant Ratios)
python run_analytics_local.py

# 4. Run PySpark Unit Tests
python -m unittest discover tests/
```

#### B. Cluster Execution with Docker (HDFS + Spark)
```bash
# 1. Start cluster containers
docker compose up -d

# 2. Upload raw CSV to HDFS
docker exec -it airwatch-namenode hdfs dfs -mkdir -p /data/air_quality/raw/station_hour
docker exec -it airwatch-namenode hdfs dfs -put -f /workspace/data/station_hour.csv /data/air_quality/raw/station_hour/

# 3. Submit PySpark Ingestion Job inside cluster
docker exec -it -e PYTHONPATH=/workspace airwatch-spark-master /spark/bin/spark-submit /workspace/hdfs_spark_ingest.py

# 4. Verify HDFS blocks, replication, and storage health
docker exec -it airwatch-namenode bash /workspace/hdfs_verification.sh
```

---

## 📁 Repository Structure

```
airwatch/
├── src/
│   ├── config.py             # Schema definitions, column mappings, default HDFS & local paths
│   ├── pipeline.py           # Core PySpark cleaning, CPCB classification & partition logic
│   ├── quality_checks.py     # Anomaly detection, null rate calculation, duplicate audit
│   └── __init__.py           # Package exports
├── mapreduce/
│   ├── mapper.py             # Streaming Mapper for PM2.5 per station
│   ├── reducer.py            # Streaming Reducer computing mean, min, max
│   └── run_mapreduce.sh      # Shell runner for Hadoop Streaming
├── hive/
│   ├── hive_setup.sql        # External table DDL for raw CSV, Parquet, and stations
│   └── hive_queries.sql      # HiveQL analytical queries
├── tests/
│   └── test_pipeline.py      # Automated PySpark transformation unit tests
├── inspect_dataset.py        # Dataset profiler (dimensions, dtypes, nulls, duplicates)
├── convert_to_hdfs_parquet.py# Parquet converter with CPCB classifications & City enrichment
├── benchmark_optimization.py # CSV vs Parquet scan performance & partition pruning benchmark
├── run_analytics_local.py    # Local high-performance SQL analytics runner
├── analytics_queries.py      # PySpark SQL distributed analytics runner
├── hdfs_spark_ingest.py      # Main CLI entrypoint for PySpark pipeline
├── hdfs_verification.sh      # HDFS inspection, fsck, and dfsadmin report script
├── docker-compose.yml        # Multi-node HDFS (NameNode, DataNode) + Spark cluster
├── hadoop.env                # Hadoop configuration and replication settings
├── requirements.txt          # Python dependencies
└── .gitignore                # Excludes 200MB+ data files from Git
```

---

## 🌐 Web Dashboards (when Docker is running)
- **HDFS NameNode UI**: [http://localhost:9870](http://localhost:9870)
- **Spark Master UI**: [http://localhost:8085](http://localhost:8085)
- **HDFS DataNode UI**: [http://localhost:9864](http://localhost:9864)
