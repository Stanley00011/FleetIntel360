# FleetIntel360

**Real-time Fleet Telemetry Analytics Platform**  
*From Raw Sensor Data to Actionable Business Intelligence*

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/warehouse-DuckDB-yellow.svg)](https://duckdb.org/)
[![Streamlit](https://img.shields.io/badge/dashboard-Streamlit-red.svg)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Live Dashboard:** [fleetintel360.streamlit.app](https://fleetintel360-ysuogxo9vb4xcbf6jnqj2h.streamlit.app/)
**Technical Documentation:** [FleetIntel360-Technical-Documentation](https://secret-hunter-438.notion.site/FleetIntel360-Technical-Documentation-2ed27ce50c4c809cb5c3e7899f81a815?pvs=143/)

---

## Project Overview

FleetIntel360 is a **production-grade data engineering project** that simulates, processes, and analyzes fleet telemetry data from vehicles and drivers. Inspired by **Formula 1's real-time telemetry systems** (which stream vehicle data using technologies like Apache Kafka), this project demonstrates a complete data pipeline—from raw sensor simulation to executive dashboards and automated alerting.

### **The Motivation**

As a data analyst who has spent years consuming data, I wanted to **see the full engineering picture**—to understand how data flows from its source through transformations into the insights I analyze daily. This project represents my journey from being a **data consumer** to becoming a **data platform builder**.

### **Business Use Case**

A commercial fleet operator managing buses and cars needs:
- **Real-time operational monitoring** (vehicle health, driver fatigue, compliance)
- **Automated risk detection** (engine overheating, driver fatigue, fraud)
- **Profitability tracking** (revenue vs. costs per driver/vehicle)
- **Data quality assurance** (freshness checks, schema validation)

FleetIntel360 delivers all of this through a modern data stack running entirely on **open-source tools**.

---

## Architecture

### **High-Level System Design**

```
┌─────────────────────────────────────────────────────────────────┐
│                      SIMULATION LAYER                           │
│  Vehicle Telemetry  │  Driver Health  │  Finance Events         │
│   (180 events/day)  │  (shift-based)  │  (trip-based)           │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
                   RAW DATA LAKE (JSONL)
              warehouse/raw/{domain}/{date}.jsonl
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STAGING LAYER                              │
│  • Validation (schema enforcement, null checks)                 │
│  • Transformation (timestamp normalization, type casting)       │
│  • Quality Gates (range validation, duplicate detection)        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
                   STAGED DATA (JSONL)
              warehouse/staging/*.jsonl
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE (DuckDB)                       │
│  Schema: Star Schema (Facts + Dimensions)                       │
│  • dim_driver, dim_vehicle, dim_date                            │
│  • fact_vehicle_telemetry (grain: event-level)                  │
│  • fact_driver_shifts (grain: shift-level)                      │
│  • fact_daily_finance (grain: driver-day)                       │
│  • fact_driver_daily_metrics (aggregated KPIs)                  │
│  • fact_vehicle_daily_metrics (aggregated KPIs)                 │
└──────────────────────┬──────────────────┬───────────────────────┘
                       │                  │
                       ▼                  ▼
              ┌────────────────┐   ┌─────────────────┐
              │   ALERTING     │   │   DASHBOARD     │
              │ Slack Webhooks │   │   Streamlit     │
              │  (Real-time)   │   │  (Interactive)  │
              └────────────────┘   └─────────────────┘
```

### **Technology Stack**

| **Layer**          | **Technology**      | **Purpose**                                    |
|--------------------|---------------------|------------------------------------------------|
| **Simulation**     | Python              | Generate realistic fleet telemetry             |
| **Storage**        | JSONL Files         | Immutable raw data lake                        |
| **Warehouse**      | DuckDB              | Embedded OLAP database (columnar storage)      |
| **Transformation** | SQL (DuckDB)        | Incremental ETL with star schema modeling      |
| **Orchestration**  | Python Scripts      | Sequential pipeline execution                  |
| **Alerting**       | Slack Webhooks      | Real-time operational notifications            |
| **Visualization**  | Streamlit           | Interactive executive dashboards               |
| **Containerization** | Docker           | Reproducible deployment environments           |
| **Automation**     | GitHub Actions      | Daily data refresh via cron jobs               |

---

## Key Features

### **1. Multi-Domain Data Simulation**

Realistic data generation across three operational domains:

- **Vehicle Telemetry** (180 events/vehicle/day)
  - GPS coordinates (lat/lon)
  - Speed, heading, fuel level
  - Engine temperature, battery voltage, tire pressure
  - Anomaly injection: overheating, fuel siphoning, tire leaks, harsh braking

- **Driver Health Monitoring** (shift-based)
  - Shift hours, continuous driving time
  - Fatigue index calculation (0-1 scale)
  - Break compliance tracking
  - Automated risk alerts

- **Financial Operations** (trip-based)
  - Revenue per trip
  - Operational costs (fuel, tolls, maintenance)
  - Net profitability calculation
  - Fraud signal detection (~12% probability)

### **2. Production-Grade Data Pipeline**

- **Idempotent Processing**: Re-running the pipeline produces identical results
- **Incremental Loading**: Only new/changed data is processed
- **Schema Enforcement**: Hard validation at staging layer
- **Data Quality Gates**: Null checks, range validation, freshness monitoring
- **Audit Trails**: First/last seen timestamps on dimensions

### **3. Star Schema Data Warehouse**

Following **Kimball methodology** for dimensional modeling:

**Dimensions:**
- `dim_date` - Calendar dimension (3 years of dates)
- `dim_driver` - Driver master (SCD Type 1 with activity tracking)
- `dim_vehicle` - Vehicle master (type inference, status tracking)

**Facts:**
- `fact_vehicle_telemetry` - Raw event-level data (high cardinality)
- `fact_driver_shifts` - Shift-level health metrics
- `fact_daily_finance` - Daily financial summaries
- `fact_driver_daily_metrics` - Aggregated driver KPIs (pre-computed)
- `fact_vehicle_daily_metrics` - Aggregated vehicle KPIs (pre-computed)

### **4. Dynamic Alert System**

Threshold-driven alerts powered by a configuration table:

| **Alert Type**       | **Metric**             | **Warning** | **Critical** |
|----------------------|------------------------|-------------|--------------|
| Driver Fatigue       | avg_fatigue_index      | 0.60        | 0.80         |
| Speeding Rate        | speeding_rate          | 8%          | 12%          |
| Engine Overheating   | engine_temp_c          | 90°C        | 120°C        |
| Battery Voltage      | battery_voltage        | 11.8V       | 11.2V        |
| Fraud Detection      | fraud_alerts_count     | 1           | 3            |

**Alert Channels:**
- **Slack**: Rich formatted messages with severity color-coding
- **Dashboard**: Live alert feed on executive dashboard

### **5. Interactive Analytics Dashboard**

Five specialized views for different stakeholders:

1. **Executive Daily Health**
   - Fleet-wide KPIs (active drivers, fatigue index, profit, alerts)
   - 7-day performance trends
   - Priority action items (risky drivers, asset status)

2. **Driver Risk Monitor**
   - Individual driver deep-dive (fatigue, speeding, compliance)
   - Policy violation audit log
   - Trend analysis (fatigue patterns, shift duration)

3. **Vehicle Monitor**
   - Asset-level diagnostics (engine temp, battery, fuel)
   - Maintenance recommendations (threshold-driven)
   - Operational log with CSV export

4. **Finance & Compliance**
   - Revenue vs. cost analysis
   - Fraud alert velocity tracking
   - Risk vs. reward scatter plots (profitability correlation)
   - Loss-making driver identification

5. **Data Quality & Trust Panel**
   - Pipeline latency monitoring (data freshness)
   - Schema integrity checks (null counts)
   - Business rule validation (range violations)
   - Ingestion volume trends

---

## Getting Started

### **Prerequisites**

- Python 3.11+
- Docker & Docker Compose (optional, for containerized deployment)
- Git

### **Local Installation**

1. **Clone the repository**
```bash
git clone https://github.com/Stanley00011/FleetIntel360.git
cd FleetIntel360
```

2. **Create virtual environment**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Set up environment variables** (for Slack alerts)
```bash
# Create .env file
echo "SLACK_WEBHOOK_URL=your_webhook_url_here" > .env
```

5. **Run initial data generation** (creates 19 days of historical data)
```bash
python -m simulator.run_simulation --start-date 2026-01-01 --days 19
```

6. **Build the analytics warehouse**
```bash
python run_staging.py
```

7. **Launch the dashboard**
```bash
streamlit run dashboard/app.py
```

Dashboard will be available at `http://localhost:8501`

### **Docker Deployment**

```bash
cd infra
docker-compose up -d
```

Access dashboard at `http://localhost:8501`

---

## Project Structure

```
FleetIntel360/
├── simulator/                    # Data generation layer
│   ├── common.py                 # Shared utilities (IDs, timestamps)
│   ├── vehicle_sim.py            # Vehicle telemetry simulation
│   ├── driver_health_sim.py      # Driver fatigue/shift simulation
│   ├── finance_sim.py            # Financial event simulation
│   └── run_simulation.py         # Batch orchestrator
│
├── warehouse/
│   ├── raw/                      # Immutable JSONL data lake
│   │   ├── vehicles/             # {date}.jsonl files
│   │   ├── driver_health/
│   │   └── finance/
│   │
│   ├── staging/                  # Validated intermediate data
│   │   ├── dim_drivers.jsonl
│   │   ├── dim_vehicles.jsonl
│   │   ├── vehicles_staged.jsonl
│   │   ├── driver_health_staged.jsonl
│   │   ├── finance_daily_staged.jsonl
│   │   └── finance_trips_staged.jsonl
│   │
│   ├── analytics/                # DuckDB warehouse
│   │   └── analytics.duckdb
│   │
│   └── sql/                      # SQL transformation layer
│       ├── schema.sql            # DDL for all tables
│       ├── dimensions/           # Dimension table logic
│       ├── facts/                # Fact table ETL
│       ├── alerts/               # Alert detection queries
│       ├── quality/              # Data quality checks
│       └── seed/                 # Reference data
│
├── dashboard/                    # Streamlit application
│   ├── app.py                    # Main entry point
│   ├── pages/                    # Multi-page app structure
│   │   ├── 1_Executive_Health.py
│   │   ├── 2_Driver_Risk.py
│   │   ├── 3_Vehicle_Monitor.py
│   │   ├── 4_Finance_Compliance.py
│   │   └── 5_Data_Quality.py
│   ├── components/               # Reusable UI components
│   │   ├── charts.py
│   │   ├── filters.py
│   │   └── kpis.py
│   └── utils/                    # Helper functions
│       ├── db.py                 # DuckDB connection manager
│       └── formatting.py         # Display formatters
│
├── stage_*.py                    # Staging layer scripts
├── build_analytics.py            # DuckDB ingestion
├── run_sql.py                    # SQL orchestrator
├── run_alerts.py                 # Alert execution
├── run_staging.py                # Full pipeline runner
├── run_daily_ops.py              # Daily orchestrator
├── slack_formatter.py            # Alert formatting
│
├── infra/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── .github/
│   └── workflows/
│       └── daily_sync.yml        # Automated cron job
│
└── requirements.txt
```

---

## Data Pipeline Flow

### **Daily Operations Workflow**

```bash
python run_daily_ops.py --date 2026-01-19
```

**Execution sequence:**

1. **Simulation** → Generates raw JSONL files for the specified date
2. **Staging** → Validates and transforms raw data
3. **Warehouse Loading** → Inserts data into DuckDB facts
4. **Dimension Updates** → Updates driver/vehicle master records
5. **Metric Aggregation** → Recomputes daily KPIs (incremental)
6. **Data Quality Checks** → Validates freshness, nulls, ranges
7. **Alert Detection** → Runs alert queries and sends Slack notifications

### **Incremental Processing Logic**

The pipeline is designed to be **idempotent** and **incremental**:

```sql
-- Example: Only recompute metrics for new dates
CREATE TEMP TABLE tmp_driver_dates AS
SELECT DISTINCT driver_id, date_key 
FROM mart.fact_vehicle_telemetry 
WHERE date_key > (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics);

-- Delete existing rows for those dates
DELETE FROM mart.fact_driver_daily_metrics
WHERE (driver_id, date_key) IN (SELECT * FROM tmp_driver_dates);

-- Insert fresh calculations
INSERT INTO mart.fact_driver_daily_metrics ...
```

This approach:
- Only processes changed data (efficient)
- Supports backfill/corrections (delete + reinsert)
- Handles late-arriving data gracefully

---

## Sample Insights

### **From the Dashboard:**

**Executive KPIs (Latest Day):**
- 6 active drivers
- Average fleet fatigue index: 0.52 (healthy)
- Net profit: $4,823
- 2 vehicles flagged for overheating
- 0 fraud alerts

**Driver Risk Findings:**
- Driver DR_003: 4 consecutive days with fatigue >0.70 (requires intervention)
- Driver DR_005: Speeding rate 15% (above 12% threshold)

**Vehicle Diagnostics:**
- BUS_01: Engine temp averaging 103°C (critical threshold: 120°C)
- CAR_02: Battery voltage at 11.6V (warning threshold: 11.8V)

**Financial Analysis:**
- Top earner: DR_002 ($1,234 net profit over 7 days)
- Loss-making driver: DR_006 (-$89, requires cost review)
- Fraud signal detected: DR_004 (3 trips flagged)

---

## Alert Examples

### **Slack Alert Format:**

```
🚨 Fleet Alert

2 alert(s) detected
────────────────────────────────

🚨 CRITICAL
Entity: BUS_01
Metric: engine_temp_c = 105.3
Info: Engine overheating risk

⚠️ WARNING
Entity: DR_003
Metric: avg_fatigue_index = 0.72
Info: Driver fatigue risk

[View Dashboard Button]
```

---

## Data Quality Framework

The project implements **three levels of data quality checks**:

### **1. Staging Validation (Hard Stops)**
- Required field checks (event_id, timestamps, IDs)
- Type validation (numeric ranges, date formats)
- Business rule validation (speed ≥0, fuel 0-100%)

### **2. Warehouse Quality Checks (Monitoring)**
```sql
-- Freshness check
SELECT DATEDIFF('day', MAX(date_key), CURRENT_DATE) as days_lag
FROM mart.fact_driver_daily_metrics;

-- Null check
SELECT COUNT(*) FROM mart.fact_vehicle_telemetry
WHERE vehicle_id IS NULL OR driver_id IS NULL;

-- Range violations
SELECT * FROM mart.fact_driver_daily_metrics
WHERE avg_speed_kph > 180 
   OR total_shift_hours > 24 
   OR avg_fatigue_index > 1;
```

### **3. Alert-Based Quality (Active Response)**
- Data freshness SLA breach (lag >1 day)
- No driver activity detection
- Ghost asset identification (vehicles with NULL last_seen_at)

---

## Cloud Migration Roadmap (Coming Soon)

The local version demonstrates the **data engineering fundamentals**. The next phase will migrate to a **cloud-native architecture** using:

- **Simulation**: GitHub Actions (scheduled workflows)
- **Ingestion**: Google Pub/Sub (real-time streaming)
- **Warehouse**: Google BigQuery (petabyte-scale analytics)
- **Transformation**: dbt Cloud (SQL-based ELT)
- **Orchestration**: BigQuery Scheduled Queries
- **Visualization**: Streamlit Cloud (deployed dashboard)

**Why this stack?**
- **Scalability**: Handle 1000+ vehicles without code changes
- **Cost Efficiency**: Pay-per-query model (no idle compute)
- **Managed Services**: Focus on logic, not infrastructure
- **Industry Standard**: Skills transferable to enterprise environments

*Follow this repo for updates on the cloud implementation!*

---

## Technical Achievements

This project demonstrates:

1. **End-to-End Ownership**: From data generation to executive dashboards
2. **Production Patterns**: Idempotency, incremental processing, error handling
3. **Dimensional Modeling**: Star schema with SCD Type 1 dimensions
4. **Modern SQL**: Window functions, CTEs, MERGE statements, JSON handling
5. **API Integration**: Slack webhooks for operational alerting
6. **DevOps Practices**: Docker containerization, GitHub Actions automation
7. **Data Quality Engineering**: Multi-layered validation framework
8. **User-Centric Design**: Role-based dashboards (executive, operations, compliance)

---

## Contributing

This is a personal portfolio project, but feedback and suggestions are welcome! Feel free to:
- Open an issue for bugs or feature requests
- Fork the repo for your own experiments
- Share insights 

---

## License

MIT License - See [LICENSE](LICENSE) file for details

---

## Acknowledgments

**Inspiration:**
- Formula 1 telemetry systems (real-time vehicle monitoring)
- Uber's fleet management platforms
- Logistics companies using IoT for operational intelligence

**Technologies:**
- [DuckDB](https://duckdb.org/) - Amazing embedded analytics database
- [Streamlit](https://streamlit.io/) - Rapid dashboard development
- [Python](https://www.python.org/) - Data engineering workhorse

---

## Connect

**Olajide Ajao **  
Data Analyst → Data Engineer  
[LinkedIn](https://www.linkedin.com/in/olajide-ajao/) | [GitHub](https://github.com/Stanley00011)

*"From consuming data to building data platforms - one pipeline at a time."*

---

** If you found this project valuable, star the repository!**
