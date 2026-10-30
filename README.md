# AirWatch: Indian Air Quality Big Data Engineering & Analytics Pipeline

Distributed big data engineering and analytics pipeline for the **Indian Air Quality Dataset** (`station_hour.csv`, **2.59 million records**) using **Hadoop HDFS**, **Apache Spark**, **Apache Parquet**, **MapReduce**, **Hive**, and an **Interactive Web Dashboard UI**.

---

## 🖥️ Interactive Web Dashboard

AirWatch features a rich, dark/light adaptive dashboard UI connected directly to the curated Parquet dataset and HDFS cluster.

```
                    ┌───────────────────────────────┐
                    │  station_hour.csv (209.5 MB)   │
                    └──────────────┬────────────────┘
                                   │  Ingest & Classify (CPCB)
                                   ▼
                    ┌───────────────────────────────┐
                    │ HDFS Parquet / Local Parquet  │
                    │  (year=YYYY / month=MM / Snappy)│
                    └──────────────┬────────────────┘
                                   │  DuckDB / Spark SQL
                                   ▼
                    ┌───────────────────────────────┐
                    │     serve_dashboard.py        │
                    │   (REST APIs + Live Stream)   │
                    └──────────────┬────────────────┘
                                   │  http://localhost:8050
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        AirWatch Web Dashboard                          │
├─────────────────┬───────────────────┬───────────────────┬──────────────┤
│ 1. Overview     │ 2. Where It's     │ 3. When It's      │ 4. Big Data  │
│                 │    Worst          │    Worst          │    Pipeline  │
│ • AQI Bar Strip │ • Top 10 Cities   │ • Monthly Heatmap │ • Data Flow  │
│ • CPCB Classes  │ • Top 10 Stations │ • Yearly Curves   │ • 25.6x Perf │
│ • Live KPIs     │ • Severe AQI Hours│ • 24-hr Diurnal   │ • HDFS Health│
└─────────────────┴───────────────────┴───────────────────┴──────────────┘
```

### Dashboard Tabs
1. **Overview**: Total AQI category distribution stacked strip, dynamic KPIs (2.59M records, 110 stations, 2,009 calendar days, 209.5 MB CSV, 25.6x speedup), dynamic data takeaways, and CPCB pollutant cross-composition table.
2. **Where it's worst**: Average PM2.5 horizontal bar chart by city (Top 10), and most polluted monitoring stations table with severe AQI hour totals.
3. **When it's worst**: Month-by-month yearly heatmap (2015–2020), multi-year seasonal trend curves, and 24-hour diurnal pollution cycle.
4. **Big data pipeline**: Architecture flow, design decisions (why 67 partitions avoid the HDFS Small Files Problem), live HDFS NameNode and Spark cluster health monitor, query scan speedup benchmark (25.6x faster), and missing data profiler.

### Launching the Dashboard

#### Option 1: Instant Local Mode (Python Server)
```bash
# 1. Export or refresh the dashboard data from Parquet
python export_dashboard_data.py

# 2. Launch the live dashboard server (automatically opens http://localhost:8050)
python serve_dashboard.py
```

#### Option 2: Full Cluster Mode with Docker
```bash
# Starts HDFS NameNode, DataNode, Spark Master, Spark Worker, and Dashboard UI
docker compose up -d

# Open http://localhost:8050 in your browser
```

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

# 4. Launch Interactive Web Dashboard
python serve_dashboard.py

# 5. Run Automated Unit & Pipeline Tests
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
├── web/
│   ├── index.html            # Standalone responsive AirWatch dashboard UI
│   └── dashboard_data.json   # Static web dataset mirror
├── mapreduce/
│   ├── mapper.py             # Streaming Mapper for PM2.5 per station
│   ├── reducer.py            # Streaming Reducer computing mean, min, max
│   └── run_mapreduce.sh      # Shell runner for Hadoop Streaming
├── hive/
│   ├── hive_setup.sql        # External table DDL for raw CSV, Parquet, and stations
│   └── hive_queries.sql      # HiveQL analytical queries
├── tests/
│   ├── test_pipeline.py      # Automated PySpark transformation unit tests
│   └── test_dashboard.py     # Dashboard data schema & API server integration tests
├── serve_dashboard.py        # Live HTTP backend server with HDFS cluster status & API endpoints
├── export_dashboard_data.py  # DuckDB & Spark exporter producing dashboard_data.json
├── index.html                # Root entrypoint for web dashboard
├── dashboard_data.json       # Live aggregated analytics payload
├── inspect_dataset.py        # Dataset profiler (dimensions, dtypes, nulls, duplicates)
├── convert_to_hdfs_parquet.py# Parquet converter with CPCB classifications & City enrichment
├── benchmark_optimization.py # CSV vs Parquet scan performance & partition pruning benchmark
├── run_analytics_local.py    # Local high-performance SQL analytics runner
├── analytics_queries.py      # PySpark SQL distributed analytics runner
├── hdfs_spark_ingest.py      # Main CLI entrypoint for PySpark pipeline
├── hdfs_verification.sh      # HDFS inspection, fsck, and dfsadmin report script
├── docker-compose.yml        # Multi-node HDFS (NameNode, DataNode) + Spark cluster + Dashboard
├── hadoop.env                # Hadoop configuration and replication settings
├── requirements.txt          # Python dependencies
└── .gitignore                # Excludes 200MB+ data files from Git
```

---

## 🌐 Web Dashboards & Endpoints
- **AirWatch Interactive UI**: [http://localhost:8050](http://localhost:8050)
  - Live Data API: [http://localhost:8050/api/dashboard-data](http://localhost:8050/api/dashboard-data)
  - HDFS Cluster Health: [http://localhost:8050/api/hdfs-status](http://localhost:8050/api/hdfs-status)
  - Live Performance Benchmark: [http://localhost:8050/api/benchmark](http://localhost:8050/api/benchmark)
- **HDFS NameNode UI**: [http://localhost:9870](http://localhost:9870)
- **Spark Master UI**: [http://localhost:8085](http://localhost:8085)
- **HDFS DataNode UI**: [http://localhost:9864](http://localhost:9864)
