# AirWatch: Comprehensive Big Data Engineering & Analytical Features Report

## 1. Executive Summary

The **AirWatch** platform is an end-to-end distributed big data analytics and engineering pipeline designed for large-scale environmental monitoring. Operating on the **Indian Air Quality Dataset** (`station_hour.csv`), the platform ingests, sanitizes, curates, partitions, and analyzes **2,589,083 hourly records** across **110 monitoring stations** in **25+ cities**, spanning **2,009 calendar days** (January 2015 through July 2020).

Environmental time-series data at this scale exhibits severe big data challenges:
- High null density in trace gases and organic volatile compounds (up to 80.15% missingness in Xylene).
- Sensor drift, power anomalies, and hardware error codes (negative readings such as `-999`).
- Severe regional heterogeneity and non-linear seasonal spikes (e.g., winter atmospheric inversions).
- The **HDFS Small Files Problem**, which rapidly degrades Hadoop NameNode performance if partitioning is poorly architected.

To address these challenges, AirWatch implements a modern decoupled storage-and-compute architecture featuring **Hadoop HDFS**, **Apache Parquet**, **PySpark SQL**, **DuckDB**, **Apache Hive**, **Hadoop Streaming MapReduce**, and an **Interactive Web Dashboard UI**.

---

## 2. Distributed Storage & Data Lake Architecture (HDFS & Parquet)

### 2.1 Storage Layer Layout
AirWatch separates storage into two distinct HDFS layers:

```
HDFS Root (/data/air_quality/)
├── raw/
│   └── station_hour/
│       └── station_hour.csv                  [209.5 MB Raw Ingestion Landing Layer]
├── metadata/
│   └── stations/
│       └── stations.csv                      [Station Geography & Metadata]
└── curated/
    └── station_hour/                         [Columnar, Partitioned Parquet Layer]
        ├── year=2015/
        │   ├── month=1/part-*.parquet
        │   └── ...
        ├── year=2016/...
        ├── ...
        └── year=2020/
            ├── month=1/part-*.parquet
            └── ...
```

### 2.2 Partitioning Strategy & The HDFS Small Files Problem
A central engineering decision in distributed systems is selecting appropriate partition granularities. Partitioning directly impacts HDFS NameNode memory, RPC scheduling overhead, and query scan performance:

| Partitioning Candidate | Directory / File Count | Assessment | Engineering Rationale |
| :--- | :--- | :--- | :--- |
| **By Hour (`year/month/day/hour`)** | ~48,000 directories | ❌ **Severe Anti-pattern** | Generates tens of thousands of tiny files (<10 KB each). Exhausts NameNode JVM heap memory and cripples Spark task scheduling. |
| **By Day (`year/month/day`)** | ~2,009 directories | ❌ **Anti-pattern** | Files remain too small (<25 KB), leading to block allocation thrashing. |
| **By Station (`StationId`)** | ~110 directories | ⚠️ **Sub-optimal** | Causes severe partition skew (some stations monitored for 5 years, others for only 3 months). Prevents temporal partition pruning. |
| **By Year & Month (`year`, `month`)** | **67 directories** | ✅ **Optimal (AirWatch Standard)** | Creates evenly sized Parquet files (~600 KB to 1.5 MB compressed). Eliminates the Small Files Problem while enabling instant temporal pruning for annual and seasonal queries. |

### 2.3 Columnar Storage & Compression Efficiency
Converting raw CSV to Snappy-compressed Apache Parquet achieved an **~80% reduction in storage footprint**:
- **Raw CSV Size**: `209.5 MB` (uncompressed text)
- **Curated Parquet Size**: `42.1 MB` (Snappy compression, dictionary encoding)
- **Column Pruning Efficiency**: Queries analyzing only particulate pollution (e.g., $PM_{2.5}$ and AQI) read only 2 columns instead of loading all 16 columns from disk, reducing memory I/O by **87.5%**.

```
CSV Row-Oriented Layout (Reads all columns for every row):
[Row 1: StationId, Datetime, PM2.5, PM10, NO, NO2, NOx, NH3, CO, SO2, O3, Benzene, Toluene, Xylene, AQI, Bucket]
[Row 2: StationId, Datetime, PM2.5, PM10, NO, NO2, NOx, NH3, CO, SO2, O3, Benzene, Toluene, Xylene, AQI, Bucket]

Parquet Columnar Layout (Reads only requested columns):
[PM2_5 Chunk:  110.8, 109.6, 104.9, 103.9, 67.3, ...]  <-- Scanned
[AQI Chunk:    239.0, 217.0, 211.0, 213.0, 454.0, ...]  <-- Scanned
[Xylene Chunk: ... ]                                     <-- Skipped entirely (0 I/O)
```

---

## 3. Data Quality Engineering & ETL Transformations

The pipeline implements strict data cleaning rules in both PySpark (`src/pipeline.py`) and vectorized Pandas/PyArrow (`convert_to_hdfs_parquet.py`):

### 3.1 Composite Key Deduplication
- **Primary Composite Key**: `(StationId, Datetime)`
- **Rule**: Eliminates duplicate sensor transmissions. In the raw dataset, all 2,589,083 records are unique on this composite key, confirming zero duplicate transmissions.

### 3.2 Physical Sensor Anomaly Rectification
- Sensor hardware faults, power interruptions, and calibration offsets often emit error codes such as `-999.0` or small negative values (`-2.4`).
- **Transformation**: All values where $\text{pollutant} < 0$ are converted to `NULL` (`np.nan`). Rows are preserved so that valid measurements of other pollutants in the same hour are not discarded.

### 3.3 Central Pollution Control Board (CPCB) Standard Classification
The pipeline dynamically evaluates hourly concentrations and generates official CPCB category labels:

$$\text{PM2\_5\_Class}(x) = \begin{cases} 
\text{Good} & x \le 30 \\ 
\text{Satisfactory} & 30 < x \le 60 \\ 
\text{Moderate} & 60 < x \le 90 \\ 
\text{Poor} & 90 < x \le 120 \\ 
\text{Very Poor} & 120 < x \le 250 \\ 
\text{Severe} & x > 250 
\end{cases}$$

$$\text{PM10\_Class}(x) = \begin{cases} 
\text{Good} & x \le 50 \\ 
\text{Satisfactory} & 50 < x \le 100 \\ 
\text{Moderate} & 100 < x \le 250 \\ 
\text{Poor} & 250 < x \le 350 \\ 
\text{Very Poor} & 350 < x \le 430 \\ 
\text{Severe} & x > 430 
\end{cases}$$

### 3.4 Schema Normalization & Metadata Enrichment
1. **Identifier Sanitization**: Column `PM2.5` is renamed to `PM2_5` to prevent parser failures in Hive, Spark SQL, and DuckDB.
2. **Metadata Join**: Broadcast joins `data/stations.csv` to enrich each hourly record with station attributes: `City`, `State`, and `StationName`.
3. **Temporal Features**: Parses UTC timestamp and projects `year`, `month`, `day`, `hour`, and `day_of_week`.

---

## 4. Multi-Engine Distributed Processing Framework

AirWatch supports four distinct big data query engines over the same curated data lake:

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                         Curated Parquet Data Lake                              │
│                (HDFS: /data/air_quality/curated/station_hour/)                 │
└───────┬────────────────────┬─────────────────────┬────────────────────┬────────┘
        │                    │                     │                    │
        ▼                    ▼                     ▼                    ▼
┌──────────────┐     ┌──────────────┐      ┌──────────────┐     ┌──────────────┐
│ Apache Spark │     │    DuckDB    │      │ Apache Hive  │     │ Hadoop Map-  │
│   (PySpark)  │     │ (Vectorized) │      │  (Metastore) │     │    Reduce    │
├──────────────┤     ├──────────────┤      ├──────────────┤     ├──────────────┤
│ Distributed  │     │ In-memory    │      │ External DDL │     │ Streaming    │
│ Cluster ETL  │     │ Sub-second   │      │ Enterprise   │     │ Map-Shuffle- │
│ & Analytics  │     │ Interactive  │      │ Data Ware-   │     │ Sort-Reduce  │
│              │     │ Dashboard    │      │ housing      │     │ Pipeline     │
└──────────────┘     └──────────────┘      └──────────────┘     └──────────────┘
```

1. **Apache Spark SQL** (`analytics_queries.py`, `hdfs_spark_ingest.py`):
   - Executes distributed query plans across Spark worker nodes.
   - Pushes partition filters down to the HDFS block reader.
2. **DuckDB Vectorized SQL** (`run_analytics_local.py`, `export_dashboard_data.py`):
   - Uses zero-copy memory mapping directly against Parquet files.
   - Executes analytical queries across 2.59 million rows in **~1.5 to 2.0 seconds** without requiring a JVM cluster.
3. **Apache Hive** (`hive/hive_setup.sql`, `hive/hive_queries.sql`):
   - Defines enterprise data catalog tables (`raw_station_hour`, `stations`, `curated_station_hour`).
   - Uses `MSCK REPAIR TABLE` to automatically discover partitioned directories.
4. **Hadoop Streaming MapReduce** (`mapreduce/mapper.py`, `mapreduce/reducer.py`):
   - Pure distributed Map-Shuffle-Sort-Reduce pipeline processing raw CSV input streams across HDFS blocks.

---

## 5. Detailed Working of Hadoop MapReduce & Partitioned Parquet

### 5.1 Deep-Dive: Working of Hadoop Streaming MapReduce

The MapReduce implementation in AirWatch (`mapreduce/mapper.py` and `mapreduce/reducer.py`) computes per-station baseline statistics (total readings, mean $PM_{2.5}$, minimum $PM_{2.5}$, and maximum $PM_{2.5}$) without loading the dataset into memory:

```
┌────────────────────────────────────────────────────────────────────────┐
│                   HDFS Raw CSV (209.5 MB, 2.59M Rows)                  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ InputSplits (128 MB HDFS Blocks)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ MAP PHASE (mapper.py)                                                  │
│ • Reads line-by-line via sys.stdin                                     │
│ • Tokenizes CSV: parts = line.split(',')                               │
│ • Filters header row & negative sensor anomalies (< 0)                 │
│ • Emits Key-Value tuples: StationId \t PM2.5 \t 1                      │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Map Output Stream
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ SHUFFLE & SORT PHASE (Hadoop Core Framework)                           │
│ • Hash Partitioner: assigns key to Reducer: hash(StationId) % N        │
│ • In-Memory Buffer Sorting: quicksorts keys within sort buffer         │
│ • Spill Files & Network Transfer (Shuffle): merges sorted runs         │
│ • Guarantee: All records for identical StationId arrive contiguous     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Partitioned & Sorted Key Stream
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ REDUCE PHASE (reducer.py)                                              │
│ • Streaming State Machine: tracks current_station in O(1) memory       │
│ • Running Accumulation: sum_pm25 += val, count++, min/max updates      │
│ • Key Change Transition: emits StationId \t Count \t Mean \t Min \t Max│
│ • Final Flush upon EOF                                                 │
└────────────────────────────────────────────────────────────────────────┘
```

#### Detailed Phase Mechanics:

1. **InputSplit & RecordReader**:
   - The HDFS NameNode slices `station_hour.csv` into physical 128 MB blocks.
   - `LineRecordReader` streams individual text lines through Unix standard input (`sys.stdin`) into child mapper processes.
   - Cross-block line handling: If a line crosses a 128 MB block boundary, the record reader automatically reads into the next block across the cluster network to complete the row.

2. **Map Phase (`mapper.py`)**:
   - **Signature**: $(K_1, V_1) \to \text{list}(K_2, V_2)$ where $K_1 = \text{byte offset}$, $V_1 = \text{raw CSV line}$, $K_2 = \text{StationId}$, $V_2 = (PM_{2.5}, 1)$.
   - Validates that $PM_{2.5}$ is a valid positive float ($PM_{2.5} \ge 0$).
   - Emits intermediate key-values:
     ```
     AP001    77.9    1
     AP001    84.2    1
     DL002    154.8   1
     ```

3. **Shuffle & Sort Phase (Distributed Coordination)**:
   - **Partitioner**: Evaluates `hash(StationId) % num_reducers` to route all occurrences of a station to the exact same Reducer node.
   - **Sort Buffer**: Mappers write to an in-memory ring buffer (default 100 MB). When 80% full, an in-memory quicksort sorts by `StationId`, and spills to disk.
   - **External Merge Sort**: On the reducer node, the framework performs a multi-way merge sort so the input to `reducer.py` is guaranteed to be strictly ordered by `StationId`.

4. **Reduce Phase (`reducer.py`)**:
   - **Memory Invariant**: Memory complexity is **strictly $O(1)$**. The reducer never buffers all records for a station into a list.
   - **Streaming Aggregation**:
     $$\text{sum\_pm25} = \sum_{i=1}^{n} x_i, \quad \text{count} = n, \quad \text{min} = \min(x_i), \quad \text{max} = \max(x_i)$$
   - **Transition**: When a new `StationId` appears, the reducer calculates $\text{mean} = \frac{\text{sum\_pm25}}{\text{count}}$ and emits the final result to HDFS:
     ```
     StationId    TotalReadings    MeanPM2.5    MinPM2.5    MaxPM2.5
     AP001        19324            77.97        0.1         999.0
     DL002        22410            154.80       1.2         985.0
     ```

---

### 5.2 Deep-Dive: Working of Partitioned Apache Parquet

Apache Parquet is an open-source, columnar storage file format optimized for fast analytical queries. In AirWatch, the data is partitioned as:

$$\text{Root} \to \text{year}=YYYY \to \text{month}=MM \to \text{part-*.parquet}$$

```
Parquet Physical File Internal Structure:

┌─────────────────────────────────────────────────────────────┐
│ Header: 'PAR1' (4-byte Magic Number)                        │
├─────────────────────────────────────────────────────────────┤
│ ROW GROUP 0 (~128 MB or 500k rows)                          │
│  ├─ Column Chunk 1: StationId (Dictionary + Bit-Packed)     │
│  │   ├─ Dictionary Page: ["AP001", "DL002", "UP016", ...]   │
│  │   └─ Data Pages: [0, 0, 0, 1, 1, 2, ...]                 │
│  ├─ Column Chunk 2: Datetime  (64-bit Timestamp Page)       │
│  ├─ Column Chunk 3: PM2_5     (32-bit Float + Snappy Page)  │
│  ├─ Column Chunk 4: PM10      (32-bit Float + Snappy Page)  │
│  └─ Column Chunk 5..16 (NO, NO2, CO, SO2, AQI, ...)         │
├─────────────────────────────────────────────────────────────┤
│ ROW GROUP 1 ...                                             │
├─────────────────────────────────────────────────────────────┤
│ FILE FOOTER (Read First by Query Engine)                    │
│  ├─ File Metadata & Schema Definition                       │
│  ├─ Row Group Metadata & Byte Offsets                       │
│  └─ Column Chunk Statistics:                                │
│      • PM2_5: [min=0.0, max=985.0, null_count=1240]         │
│      • AQI:   [min=12.0, max=500.0, null_count=980]         │
├─────────────────────────────────────────────────────────────┤
│ 4-byte Footer Length + 'PAR1' Magic Number                  │
└─────────────────────────────────────────────────────────────┘
```

#### Why Partitioned Parquet Achieves a 25.6× Speedup:

1. **Partition Pruning (Directory Pushdown)**:
   - When a query contains `WHERE year = 2020`, query engines (Spark SQL, DuckDB, Hive) evaluate directory names before opening files.
   - Directories matching `year=2015/` through `year=2019/` (60 out of 67 partition directories) are **completely skipped**. Zero bytes are transferred or read for 89.5% of the dataset.

2. **Column Projection (Vertical Partitioning)**:
   - In CSV, every row must be parsed sequentially from left to right. To read $PM_{2.5}$ and AQI, the computer must parse all other 14 columns.
   - In Parquet, the engine seeks directly to the byte offsets of the `PM2_5` and `AQI` column chunks specified in the Footer. The other 14 column chunks are never touched.

3. **Page-Level Statistics & Row-Group Skipping**:
   - The Parquet Footer stores `[min_value, max_value]` for each column chunk.
   - When searching for severe pollution ($PM_{2.5} > 250$), any Row Group whose footer stats report $\text{max}(PM_{2.5}) < 250$ is instantly bypassed without decompression.

4. **Dictionary Encoding & Snappy Compression**:
   - **Dictionary Encoding**: Repeated strings like `City = "Delhi"` or `AQI_Bucket = "Severe"` are replaced with 1-byte integer tokens, reducing memory by ~75%.
   - **Snappy Compression**: High-throughput block compressor decompressing at $>250\text{ MB/s}$ per CPU core with low CPU latency.

---

## 6. Automated Severe Exposure Alerting Engine (>6 Consecutive Hours)

### 6.1 Environmental Threat & Problem Formulation
Prolonged human exposure to air categorized as **CPCB Severe** ($PM_{2.5} > 250\ \mu\text{g/m}^3$ or $\text{AQI} > 400$) causes acute pulmonary stress, severe asthma exacerbations, and cardiovascular crises. While episodic 1-hour spikes can occur due to local dust or sensor noise, continuous exposure lasting **$> 6$ consecutive hours** represents an acute public health emergency requiring immediate public advisories and industrial curtailment.

### 6.2 Consecutive Streak Detection Algorithm (`src/alerts.py`)
To detect multi-hour severe streaks across 2.59 million rows without memory limits, AirWatch implements an optimized SQL window grouping algorithm:

```sql
WITH flagged AS (
    SELECT 
        year, StationId, StationName, City, Datetime, PM2_5, AQI,
        CASE WHEN (PM2_5 > 250.0 OR AQI_Bucket = 'Severe') THEN 1 ELSE 0 END AS is_severe,
        LAG(Datetime) OVER (PARTITION BY StationId ORDER BY Datetime) AS prev_dt,
        LAG(is_severe) OVER (PARTITION BY StationId ORDER BY Datetime) AS prev_severe
    FROM aq
),
continuous_blocks AS (
    SELECT *,
        CASE 
            WHEN is_severe = 1 AND (prev_severe = 0 OR prev_severe IS NULL OR epoch(Datetime) - epoch(prev_dt) > 3600) THEN 1 
            ELSE 0 
        END AS is_new_streak
    FROM flagged
    WHERE is_severe = 1
),
grouped AS (
    SELECT *,
        SUM(is_new_streak) OVER (PARTITION BY StationId ORDER BY Datetime) AS streak_group
    FROM continuous_blocks
)
SELECT 
    StationId, FIRST(StationName) AS StationName, FIRST(City) AS City,
    MIN(Datetime) AS streak_start, MAX(Datetime) AS streak_end,
    COUNT(*) AS consecutive_hours,
    ROUND(AVG(PM2_5), 1) AS avg_pm25,
    ROUND(MAX(PM2_5), 1) AS peak_pm25
FROM grouped
GROUP BY StationId, streak_group
HAVING COUNT(*) >= 6
ORDER BY streak_end DESC, consecutive_hours DESC;
```

#### Alert Severity Matrix:
| Consecutive Hours | Alert Level | Actionable Advisory |
| :---: | :---: | :--- |
| **6 to 11 Hours** | `WARNING` | Vulnerable groups (children, elderly, respiratory patients) remain indoors. |
| **12 to 23 Hours** | `CRITICAL` | General public must avoid outdoor exertion; air purifiers recommended. |
| **$\ge$ 24 Hours** | `EMERGENCY` | Full industrial & construction shutdown; traffic restrictions triggered. |

### 6.3 Multi-Channel Notification Dispatchers
1. **Slack / Generic Webhooks** (`send_slack_webhook`):
   - Formats structured Slack Block Kit payloads with colored status indicators (Warning: `#EF8A2A`, Critical: `#D9482F`, Emergency: `#8E1B2E`).
   - Dispatches via HTTP POST to `AIRWATCH_WEBHOOK_URL`.
2. **Email Alerts (SMTP)** (`send_email_alert`):
   - Generates dual MIME-encoded HTML and plaintext emails with station details, duration, peak values, and health warnings.
3. **Audit Logging** (`save_alerts_audit_log`):
   - Persists all detected historical incidents to `alerts_log.json` for compliance auditing.

---

## 7. In-Depth Analytical Features & Empirical Findings

### Feature 1: Spatial Pollution Disparity (City-Level Rankings)

#### Analytical Objective
Identify the most polluted urban agglomerations across India by calculating the volume-weighted average concentrations of $PM_{2.5}$, $PM_{10}$, and overall AQI, enforcing a significance threshold of $>5,000$ hourly readings.

#### Query Implementation (Spark SQL / DuckDB)
```sql
SELECT 
    COALESCE(City, 'Unknown') AS City,
    COUNT(DISTINCT StationId) AS ActiveStations,
    COUNT(*) AS HourlyReadings,
    ROUND(AVG(PM2_5), 2) AS Avg_PM25,
    ROUND(AVG(PM10), 2) AS Avg_PM10,
    ROUND(AVG(AQI), 2) AS Avg_AQI
FROM air_quality
WHERE City IS NOT NULL
GROUP BY City
HAVING COUNT(*) > 5000
ORDER BY Avg_PM25 DESC
LIMIT 10;
```

#### Empirical Findings
| Rank | City | Active Stations | Hourly Readings | Avg PM2.5 ($\mu\text{g/m}^3$) | Avg PM10 ($\mu\text{g/m}^3$) | Avg AQI | CPCB Classification |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| **1** | **Delhi** | 38 | 1,087,388 | **110.85** | 216.61 | **238.93** | Poor |
| **2** | **Lucknow** | 5 | 146,210 | **109.62** | *NA* | **217.01** | Poor |
| **3** | **Gurugram** | 4 | 67,804 | **104.91** | 171.96 | **210.78** | Poor |
| **4** | **Patna** | 6 | 64,031 | **103.93** | 121.23 | **212.91** | Poor |
| **5** | **Ahmedabad** | 1 | 48,192 | **67.27** | 111.49 | **453.54** | Moderate |
| **6** | **Brajrajnagar**| 1 | 22,468 | **66.45** | 125.42 | **151.42** | Moderate |
| **7** | **Jorapokhar** | 1 | 28,025 | **63.97** | 149.72 | **159.99** | Moderate |
| **8** | **Talcher** | 1 | 22,161 | **63.91** | 171.77 | **177.56** | Moderate |
| **9** | **Guwahati** | 1 | 12,002 | **61.78** | 113.88 | **140.25** | Moderate |
| **10**| **Kolkata** | 7 | 75,717 | **58.85** | 109.80 | **132.59** | Satisfactory |

#### Key Insights
1. **The Indo-Gangetic Plain (IGP) Pollution Trap**: The top 4 most polluted cities (Delhi, Lucknow, Gurugram, Patna) are all located within the Indo-Gangetic Basin, suffering from stagnant winter air masses, agricultural stubble burning, and high vehicular density.
2. **Coastal & Peninsular Clean Air**: Southern and coastal metropolises (Mumbai: 41.5 $\mu\text{g/m}^3$, Hyderabad: 40.2 $\mu\text{g/m}^3$, Bengaluru: 33.1 $\mu\text{g/m}^3$) exhibit cleaner air due to consistent maritime sea breezes and thermal convection.

---

### Feature 2: High-Severity Station Hotspots & Extreme Event Auditing

#### Analytical Objective
Identify the most hazardous individual monitoring stations across India by calculating mean particulate levels, maximum recorded spikes, and the total accumulated hours categorized as **Severe** (AQI $> 400$).

#### Query Implementation
```sql
SELECT 
    StationId,
    COALESCE(StationName, StationId) AS StationName,
    COALESCE(City, 'Unknown') AS City,
    ROUND(AVG(PM2_5), 1) AS Mean_PM25,
    ROUND(MAX(PM2_5), 1) AS Max_PM25,
    SUM(CASE WHEN AQI_Bucket = 'Severe' THEN 1 ELSE 0 END) AS Severe_AQI_Hours
FROM air_quality
GROUP BY StationId, StationName, City
HAVING COUNT(PM2_5) > 1000
ORDER BY Mean_PM25 DESC
LIMIT 10;
```

#### Empirical Findings
| Station ID | Station Name | City | Mean PM2.5 | Max PM2.5 | Severe AQI Hours | Exposure Impact |
| :--- | :--- | :--- | :---: | :---: | :---: | :--- |
| **DL002** | Anand Vihar, Delhi - DPCC | Delhi | **154.8** | 985.0 | **13,320** | Chronic Severe Zone (Major bus terminal & highway junction) |
| **UP016** | Talkatora District Ind. Center | Lucknow | **134.6** | 1,000.0 | 2,395 | Heavy Industrial Emission Zone |
| **DL008** | DTU, Delhi - CPCB | Delhi | **132.1** | 1,000.0 | 2,965 | Northern Highway Corridor |
| **BR007** | IGSC Planetarium Complex | Patna | **129.7** | 994.0 | 3,871 | Dense Commercial Center |
| **DL015** | Jahangirpuri, Delhi - DPCC | Delhi | **129.0** | 1,000.0 | 3,102 | Industrial & High Traffic Hub |
| **DL038** | Wazirpur, Delhi - DPCC | Delhi | **128.6** | 969.8 | 3,577 | Metal Finishing & Industrial Cluster |
| **DL020** | Mundka, Delhi - DPCC | Delhi | **123.9** | 988.5 | 3,205 | Industrial Waste & Heavy Freight Route |
| **DL032** | Rohini, Delhi - DPCC | Delhi | **123.6** | 967.0 | 3,157 | Outer Suburb with Open Biomass Burning |
| **DL005** | Bawana, Delhi - DPCC | Delhi | **121.8** | 991.0 | 2,412 | Planned Industrial Area |
| **DL006** | Burari Crossing, Delhi - IMD | Delhi | **121.8** | 989.7 | 1,432 | Entry corridor from Haryana |

#### Key Insights
1. **Sensor Upper Limit Saturation**: Max values repeatedly cluster around $985.0 - 1000.0 \mu\text{g/m}^3$, confirming that optical sensors hit their physical upper saturation limit during winter smog emergencies.
2. **Anand Vihar Anomaly**: DL002 accumulated **13,320 hours** in the "Severe" category—representing ~1.5 years of continuous hazardous air exposure.

---

### Feature 3: Temporal Dynamics (Winter Spikes & Diurnal Profiles)

#### Analytical Objective
Quantify temporal cycles across two orthogonal dimensions:
1. Multi-year seasonal trends (evaluating partition-pruned winter spikes from 2015 to 2020).
2. The 24-hour diurnal cycle (identifying daily peak exposure hours).

#### Multi-Year Monthly Trends
```
Monthly PM2.5 Average (µg/m³) across India:

170 ┼                                                  ╭──╮
150 ┼ ╭──╮                                        ╭──╮ │  │
130 ┼ │  │ ╭──╮                              ╭──╮ │  │ │  │
110 ┼ │  │ │  │                         ╭──╮ │  │ │  │ │  │
 90 ┼ │  │ │  │                    ╭──╮ │  │ │  │ │  │ │  │
 70 ┼ │  │ │  │ ╭──╮ ╭──╮ ╭──╮     │  │ │  │ │  │ │  │ │  │
 50 ┼ │  │ │  │ │  │ │  │ │  │ ╭──╮│  │ │  │ │  │ │  │ │  │
 30 ┼ │  │ │  │ │  │ │  │ │  │ │  ││  │ │  │ │  │ │  │ │  │ ╭──╮
    ┴─┴──┴─┴──┴─┴──┴─┴──┴─┴──┴─┴──┴┴──┴─┴──┴─┴──┴─┴──┴─┴──┴─┴──┴
      Jan  Feb  Mar  Apr  May  Jun Jul  Aug  Sep  Oct  Nov  Dec
     (128) (95) (64) (56) (57) (46)(36) (31) (35) (90)(137)(145)
```

- **Winter Smog Peak**: December ($145.0 \mu\text{g/m}^3$), November ($137.1 \mu\text{g/m}^3$), and January ($128.4 \mu\text{g/m}^3$) are 4.5× more polluted than monsoon months.
- **Monsoon Scavenging**: In August ($31.5 \mu\text{g/m}^3$) and July ($35.6 \mu\text{g/m}^3$), wet deposition and precipitation scrub particulates from the troposphere.

#### 24-Hour Diurnal Pollution Cycle
Aggregating all 2.59M records by hour of day ($00:00 - 23:00$) uncovers a distinct diurnal rhythm:

| Hour | Avg PM2.5 ($\mu\text{g/m}^3$) | Hour | Avg PM2.5 ($\mu\text{g/m}^3$) | Atmospheric Mechanism |
| :---: | :---: | :---: | :---: | :--- |
| **00:00** | 95.3 | **12:00** | 86.8 | Late night accumulation vs midday solar heating |
| **01:00** | 93.5 | **13:00** | 79.1 | Planetary boundary layer expands vertically |
| **02:00** | 91.4 | **14:00** | 71.8 | Thermal turbulence increases vertical mixing |
| **03:00** | 88.2 | **15:00** | 65.7 | Cleanest period of the day |
| **04:00** | 85.8 | **16:00** | 61.5 | Midday minimum |
| **05:00** | 84.3 | **17:00** | **57.8** | **Daily Minimum ($57.8 \mu\text{g/m}^3$)** |
| **06:00** | 84.1 | **18:00** | 59.5 | Evening rush hour begins |
| **07:00** | 85.7 | **19:00** | 65.9 | Rapid surface cooling & inversion begins |
| **08:00** | 89.6 | **20:00** | 76.3 | Boundary layer contracts |
| **09:00** | 93.3 | **21:00** | 86.6 | Heavy commercial trucking restriction lifted |
| **10:00** | 92.3 | **22:00** | 93.0 | Nighttime smoke trapping |
| **11:00** | 86.8 | **23:00** | **95.6** | **Daily Peak ($95.6 \mu\text{g/m}^3$)** |

**Meteorological Explanation**: Pollution is lowest at 17:00 ($57.8 \mu\text{g/m}^3$) because solar radiation drives convective mixing. After sunset, radiational cooling traps ground-level emissions within a shallow nocturnal inversion layer, driving concentrations up to $95.6 \mu\text{g/m}^3$ at 23:00.

---

### Feature 4: Pollutant Cross-Composition & Diagnostic Ratios

#### Analytical Objective
Evaluate how the chemical mixture of air changes as pollution intensifies across CPCB categories, specifically analyzing the $PM_{2.5} / PM_{10}$ ratio, Nitrogen Dioxide ($NO_2$), and Carbon Monoxide ($CO$).

#### Query Implementation
```sql
SELECT 
    PM2_5_Class,
    COUNT(*) AS Readings,
    ROUND(AVG(PM2_5), 2) AS Mean_PM25,
    ROUND(AVG(PM10), 2) AS Mean_PM10,
    ROUND(AVG(PM2_5) / NULLIF(AVG(PM10), 0), 2) AS PM25_to_PM10_Ratio,
    ROUND(AVG(NO2), 2) AS Mean_NO2,
    ROUND(AVG(CO), 2) AS Mean_CO
FROM air_quality
WHERE PM2_5_Class IS NOT NULL
GROUP BY PM2_5_Class
ORDER BY Mean_PM25 ASC;
```

#### Empirical Findings
| CPCB Class | Total Readings | Mean PM2.5 ($\mu\text{g/m}^3$) | Mean PM10 ($\mu\text{g/m}^3$) | $PM_{2.5}/PM_{10}$ Ratio | Mean $NO_2$ ($\mu\text{g/m}^3$) | Mean $CO$ ($\text{mg/m}^3$) | Dominant Source |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Good** | 529,433 | 17.79 | 55.02 | **0.32** | 19.02 | 0.92 | Natural background / coarse dust |
| **Satisfactory** | 554,211 | 43.93 | 114.10 | **0.39** | 29.53 | 1.32 | Road dust & baseline urban activity |
| **Moderate** | 322,333 | 73.71 | 170.44 | **0.43** | 39.44 | 1.56 | Mixed combustion & crustal sources |
| **Poor** | 164,918 | 103.87 | 221.83 | **0.47** | 45.51 | 2.00 | Heavy vehicular traffic & industry |
| **Very Poor** | 272,249 | 169.36 | 308.84 | **0.55** | 55.23 | 2.52 | Significant secondary aerosol formation |
| **Severe** | 98,250 | 368.72 | 521.57 | **0.71** | 71.06 | 3.13 | **Intense combustion (biomass, fires, coal)** |

#### Scientific Significance
- In clean air (**Good**), the ratio is **0.32**, meaning coarse particles ($PM_{10}$, such as mechanical soil and road dust) make up ~68% of the particulate mass.
- In hazardous air (**Severe**), the ratio jumps to **0.71**, with fine combustion particles ($PM_{2.5}$) constituting 71% of total aerosol mass.
- Concurrently, toxic combustion byproducts ($NO_2$ and $CO$) more than triple from Good to Severe, confirming that severe smog episodes are driven by **combustion aerosols** rather than natural dust storms.

---

## 6. Performance Optimization & Benchmark Evaluation

### 6.1 Benchmark Experiment
The benchmark script (`benchmark_optimization.py`) compares scanning and filtering performance on the Year 2020 dataset between:
1. **Raw CSV**: Linear text scan of `data/station_hour.csv` (209.5 MB).
2. **Partitioned Parquet**: Direct directory scan of `year=2020` partitions with columnar pushdown.

### 6.2 Quantitative Results
```
======================================================================
 AIRWATCH BENCHMARK RESULTS: RAW CSV vs PARTITIONED PARQUET
======================================================================
Raw CSV Scan & Filter Time         : 4.86 seconds
Partitioned Parquet Scan Time      : 0.19 seconds
Query Speedup Factor               : 25.6x FASTER
Storage Compression                : 79.9% space reduction
Data Filtered                      : Year = 2020 (Records: 442,118)
======================================================================
```

**Why Parquet is 25.6× Faster**:
1. **Partition Pruning**: The query engine completely ignores directories `year=2015` through `year=2019`, eliminating ~83% of file path scans.
2. **Column Projection**: Only $PM_{2.5}$ and AQI byte streams are decompressed; the other 14 columns are skipped.
3. **Snappy Decompression**: Decompresses at over 250 MB/sec per CPU core.

---

## 7. Interactive UI & Backend Integration

AirWatch bridges big data infrastructure with an interactive, presentation-ready web dashboard:

```
┌────────────────────────────────────────────────────────┐
│             AirWatch Web UI (Port 8050)                │
│    Chart.js Visualizations • Responsive Tab Views      │
└───────────────────────────┬────────────────────────────┘
                            │ Fetch / REST APIs
                            ▼
┌────────────────────────────────────────────────────────┐
│            serve_dashboard.py Server                   │
│   • /api/dashboard-data   • /api/refresh (POST)        │
│   • /api/hdfs-status      • /api/benchmark             │
└─────────────┬──────────────────────────┬───────────────┘
              │                          │
              ▼                          ▼
   ┌──────────────────────┐    ┌─────────────────────┐
   │ DuckDB SQL Engine    │    │ HDFS NameNode / JMX │
   │ (Curated Parquet)    │    │ (Port 9870 / 9000)  │
   └──────────────────────┘    └─────────────────────┘
```

1. **Auto-Synchronization**: Automatically queries the curated dataset via DuckDB and generates `dashboard_data.json`.
2. **Cluster Health Telemetry**: Live ping checks to HDFS NameNode (`hdfs://namenode:9000`), NameNode Web UI (`http://localhost:9870`), and Spark Master (`http://localhost:8085`).
3. **Live Sync Trigger**: The `Sync Backend` button executes a fresh SQL re-aggregation over the Parquet store and updates the UI without page reloads.

---

## 8. Verification & Test Suite Summary

The repository includes a comprehensive unit and integration test suite (`tests/`):
- **`tests/test_pipeline.py`**: Validates PySpark dataset cleaning, composite key deduplication, negative anomaly nullification, and CPCB classification logic.
- **`tests/test_dashboard.py`**: Validates DuckDB analytical exports, JSON schema integrity, and HTTP server REST endpoints (`/api/dashboard-data`, `/api/refresh`, `/api/hdfs-status`, `/api/benchmark`).

```bash
# Run all tests
python -m unittest discover tests/
# Output: Ran 7 tests in 70.44s - OK
```

---

## 9. Conclusion

The AirWatch big data pipeline successfully demonstrates:
1. **Scalable Storage Design**: Mitigated the HDFS Small Files Problem by selecting an optimal `(year, month)` partition scheme, cutting storage by ~80% with Snappy Parquet.
2. **Rigorous Data Cleaning**: Fixed sensor anomalies and applied official CPCB standards across 2.59 million rows.
3. **Multi-Engine Interoperability**: Successfully ran unified analytics across PySpark, DuckDB, Hive, and MapReduce.
4. **Significant Scientific Findings**: Quantified the Indo-Gangetic Plain pollution trap, extreme station exposures, winter smog dynamics, diurnal inversion cycles, and the combustion-driven shift in $PM_{2.5}/PM_{10}$ ratios during severe episodes.
5. **Real-Time Visualization**: Connected curated big data stores directly to an interactive, accessible dashboard UI.

