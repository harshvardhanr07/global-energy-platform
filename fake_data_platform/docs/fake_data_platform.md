# FAKE_DATA_PLATFORM — Complete Technical Documentation

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Folder Structure](#folder-structure)
4. [Environment Setup](#environment-setup)
5. [Docker Infrastructure](#docker-infrastructure)
6. [Data Sources](#data-sources)
7. [Module 1 — Database Seeder](#module-1--database-seeder)
8. [Module 2 — CSV Generator](#module-2--csv-generator)
9. [Module 3 — API Simulator](#module-3--api-simulator)
10. [Shared Configuration](#shared-configuration)
11. [Running the Platform](#running-the-platform)
12. [API Reference](#api-reference)
13. [Data Quality & Anomaly Rules](#data-quality--anomaly-rules)
14. [Site Reference](#site-reference)
15. [Cross-Source Consistency Rules](#cross-source-consistency-rules)

---

## Overview

The `fake_data_platform` is a standalone data generation system that produces realistic, production-like synthetic energy data to feed into the **Global Energy Platform**.

It simulates three real-world data sources found in enterprise energy management systems:

| Source | Type | Technology |
|---|---|---|
| IoT Sensors | REST API (minute-level) | FastAPI + Parquet |
| Energy Invoices | CSV files (monthly) | Pandas |
| Site Master Data | Relational DB (4 tables) | PostgreSQL |

**Scope:** 10 sites, 2 years of historical data, continuous near real-time generation.

---

## Architecture

```
fake_data_platform/
│
├── config/                    # Shared configuration and site definitions
├── db_seeder/                 # PostgreSQL master data generator
├── csv_generator/             # Monthly invoice CSV generator
├── api_simulator/             # IoT sensor API (FastAPI + Parquet)
├── output/
│   ├── csv/                   # Generated invoice CSVs
│   └── parquet/               # Generated IoT sensor Parquet files
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env
```

**Data flow:**

```
config/sites.json
      │
      ├──► db_seeder/     → PostgreSQL (4 tables)
      │
      ├──► csv_generator/ → output/csv/invoices_YYYY_MM.csv
      │
      └──► api_simulator/ → output/parquet/SITE_XXX/year=YYYY/month=MM/data.parquet
                         → FastAPI endpoints on :8000
```

---

## Folder Structure

```
fake_data_platform/
│
├── config/
│   ├── __init__.py
│   ├── sites.json                  # 10 site definitions (source of truth)
│   └── shared_config.py            # Shared: CLIMATE_ZONE_BY_COUNTRY, SEASONAL_FACTORS
│
├── db_seeder/
│   ├── __init__.py
│   ├── schema.py                   # Creates 4 PostgreSQL tables
│   ├── seed_sites.py               # Populates site_profile
│   ├── seed_history.py             # Populates site_profile_history + site_status_history
│   ├── seed_occupancy.py           # Populates site_occupancy (2 years daily)
│   └── run_seeder.py               # Entry point — runs all seeders in order
│
├── csv_generator/
│   ├── __init__.py
│   ├── csv_config.py               # Units, costs, base consumption ranges
│   ├── generator.py                # Core monthly invoice generation logic
│   ├── exporter.py                 # Writes rows to CSV files
│   └── run_generator.py            # Entry point — generates 24 monthly CSVs
│
├── api_simulator/
│   ├── __init__.py
│   ├── api_config.py               # Consumption ranges, temperature, anomaly config
│   ├── generator.py                # Minute-level data generation (invoice-aligned)
│   ├── storage.py                  # Parquet read/write/append operations
│   ├── backfill.py                 # 2-year historical backfill (vectorized)
│   ├── scheduler.py                # Hourly append job (APScheduler)
│   ├── api.py                      # FastAPI routes
│   └── main.py                     # Entry point — backfill + scheduler + API
│
├── output/
│   ├── csv/                        # invoices_YYYY_MM.csv (24 files)
│   └── parquet/                    # SITE_XXX/year=YYYY/month=MM/data.parquet
│
├── docs/
│   └── FAKE_DATA_PLATFORM.md       # Data specification document
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env
└── .env.example
```

---

## Environment Setup

### Prerequisites

- WSL2 (Ubuntu 24)
- Docker + Docker Compose
- Python 3.11+
- Git

### `.env` file

```dotenv
PROJECT_NAME=fake_data_platform
ENV=dev
TIMEZONE=UTC
START_DATE=2024-01-01
END_DATE=2025-12-31
NUM_SITES=10
DATA_FREQUENCY_MINUTES=1

POSTGRES_DB=energy_fake
POSTGRES_USER=energy_user
POSTGRES_PASSWORD=energy_pass
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

API_HOST=0.0.0.0
API_PORT=8000

PGADMIN_EMAIL=admin@energy.com
PGADMIN_PASSWORD=admin
```

### `requirements.txt`

```
fastapi
uvicorn[standard]
psycopg2-binary
pandas
numpy
faker
python-dotenv
pydantic
pyarrow
APScheduler==3.10.4
python-dateutil
```

---

## Docker Infrastructure

### Services

| Service | Image | Port | Purpose |
|---|---|---|---|
| `postgres` | postgres:15 | 5432 | Master data database |
| `pgadmin` | dpage/pgadmin4 | 5050 | DB visual inspection |
| `api_simulator` | custom (Dockerfile) | 8000 | IoT API + data generator |

### `docker-compose.yml` structure

```yaml
services:
  postgres:
    image: postgres:15
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U energy_user -d energy_fake"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - energy_network

  pgadmin:
    image: dpage/pgadmin4
    ports:
      - "5050:80"
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - energy_network

  api_simulator:
    build: .
    command: python api_simulator/main.py
    ports:
      - "8000:8000"
    volumes:
      - .:/app
      - ./output/csv:/app/output/csv
      - ./output/parquet:/app/output/parquet
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - energy_network

networks:
  energy_network:
    driver: bridge

volumes:
  postgres_data:
```

### Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["python", "api_simulator/main.py"]
```

---

## Data Sources

### Source 1 — PostgreSQL Database (Master Data)

Four tables tracking site lifecycle and daily occupancy.

**Table: `site_profile`** — Current state of each site (100% accurate)

| Column | Type | Notes |
|---|---|---|
| site_id | VARCHAR(10) PK | e.g. SITE_001 |
| site_name | VARCHAR(255) | Full site name |
| status | VARCHAR(20) | active / inactive / closed / colocated |
| active_date | DATE | When site became operational |
| inactive_date | DATE | When site closed (null if active) |
| country | VARCHAR(100) | Country name |
| city | VARCHAR(100) | City name |
| latitude | NUMERIC(9,6) | Geographic coordinates |
| longitude | NUMERIC(9,6) | Geographic coordinates |
| site_sqm | NUMERIC(10,2) | Floor area in square meters |
| site_capacity | SMALLINT | Maximum headcount |
| billing_cycle | VARCHAR(20) | calendar or mid_month |
| timezone | VARCHAR(50) | e.g. Europe/London |
| last_updated_on | TIMESTAMP | Auto-set on insert |

**Table: `site_profile_history`** — Field-level audit trail (100% accurate)

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Auto increment |
| site_id | VARCHAR(10) FK | References site_profile |
| change_field | VARCHAR(100) | Name of changed field |
| old_value | TEXT | Previous value |
| new_value | TEXT | New value |
| changed_on | TIMESTAMP | When change occurred |

**Table: `site_status_history`** — Lifecycle events (100% accurate)

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Auto increment |
| site_id | VARCHAR(10) FK | References site_profile |
| event_type | VARCHAR(20) | moved / closed / colocated / inactive_period |
| new_site_id | VARCHAR(10) FK | Destination site (nullable) |
| updated_on | TIMESTAMP | When event was recorded |

**Table: `site_occupancy`** — Daily headcount (~97% accurate)

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Auto increment |
| site_id | VARCHAR(10) FK | References site_profile |
| site_capacity | SMALLINT | Capacity at time of record |
| date | DATE | The date |
| occupancy | SMALLINT | Actual headcount |

Unique constraint on (site_id, date) prevents duplicates.
~3% of rows intentionally missing to simulate real data gaps.

---

### Source 2 — CSV Files (Invoice Data)

Monthly energy invoices from vendors, stored as CSVs.

**File naming:** `invoices_YYYY_MM.csv`
**Location:** `output/csv/`
**Coverage:** 24 files (2 years × 12 months)

**CSV Schema:**

| Column | Type | Description |
|---|---|---|
| site_id | String | Site identifier |
| site_name | String | Site name |
| billing_period_from | Date | Start of billing period |
| billing_period_to | Date | End of billing period |
| consumption_type | String | electricity / natural_gas / district_energy / diesel |
| consumption | Float | Total consumption for period |
| consumption_unit | String | kWh / m³ / GJ / MMBtu / litres / gallons / therms |
| consumption_cost | Float | Total cost |
| consumption_cost_unit | String | Currency (USD / GBP / EUR / etc.) |
| cost_per_consumption_unit | Float | Unit rate |

**Billing cycles:**
- Most sites: calendar month (1st to last day)
- SITE_002 (Paris), SITE_005 (Sydney): mid-month (15th to 14th of next month)

**Energy types per site:**

| Site | Electricity | Natural Gas | District Energy | Diesel |
|---|---|---|---|---|
| SITE_001 (London) | ✅ | ✅ | ✅ | |
| SITE_002 (Paris) | ✅ | | ✅ | |
| SITE_003 (New York) | ✅ | ✅ | | ✅ |
| SITE_004 (Tokyo) | ✅ | | ✅ | |
| SITE_005 (Sydney) | ✅ | ✅ | | |
| SITE_006 (Dubai) | ✅ | | ✅ | ✅ |
| SITE_007 (Toronto) | ✅ | ✅ | ✅ | |
| SITE_008 (Berlin) | ✅ | ✅ | ✅ | |
| SITE_009 (Singapore) | ✅ | | | ✅ |
| SITE_010 (São Paulo) | ✅ | ✅ | | ✅ |

**Data quality issues (intentional):**
- ~2% of site-months have missing invoices (simulate late vendor submissions)
- Unit rates vary slightly month to month (simulate tariff changes)
- Mid-to-mid billing sites create cross-month alignment challenges

---

### Source 3 — API (IoT Sensor Simulation)

Minute-level electricity consumption and temperature data served via FastAPI.

**Data stored as:** Parquet files partitioned by site/year/month
**Location:** `output/parquet/SITE_XXX/year=YYYY/month=MM/data.parquet`
**Rows per file:** ~44,640 (31 days × 24 hours × 60 minutes)
**Total rows:** ~10.4 million across all sites and months

**Data schema per row:**

| Column | Type | Description |
|---|---|---|
| site_id | String | Site identifier |
| timestamp | Int64 | Unix timestamp (seconds UTC) |
| heating | Float | kWh — space heating |
| cooling | Float | kWh — air conditioning |
| lighting | Float | kWh — all lighting systems |
| ventilation | Float | kWh — fans and air handling |
| ups | Float | kWh — uninterruptible power supply |
| it | Float | kWh — servers, networking, workstations |
| restaurant | Float | kWh — kitchen and cafeteria |
| avg_outside_temp | Float | °C — ambient external temperature |
| degree_day_cooling | Float | DDC — cooling degree days |
| degree_day_heating | Float | DDH — heating degree days |
| reference_temp | Float | °C — base temp for degree days (18°C) |

**Hourly append behaviour:**
- Historical months: written once during backfill, never modified
- Current month: overwritten every hour with 60 new rows appended
- Deduplication on timestamp ensures idempotent append operations

---

## Module 1 — Database Seeder

### Files

| File | Purpose |
|---|---|
| `schema.py` | Creates 4 tables with indexes and constraints |
| `seed_sites.py` | Inserts 10 sites into site_profile |
| `seed_history.py` | Inserts 23 profile changes + 2 lifecycle events |
| `seed_occupancy.py` | Generates ~7,100 daily occupancy rows (2 years) |
| `run_seeder.py` | Orchestrates all seeders in dependency order |

### How to run

```bash
# run full seeder
docker exec -it api_simulator python db_seeder/run_seeder.py

# run individual modules
docker exec -it api_simulator python db_seeder/schema.py
docker exec -it api_simulator python db_seeder/seed_sites.py
docker exec -it api_simulator python db_seeder/seed_history.py
docker exec -it api_simulator python db_seeder/seed_occupancy.py
```

### Verify

```bash
# check all tables exist
docker exec -it postgres psql -U energy_user -d energy_fake -c "\dt"

# check row counts
docker exec -it postgres psql -U energy_user -d energy_fake -c "
  SELECT 'site_profile' as tbl, COUNT(*) FROM site_profile
  UNION ALL
  SELECT 'site_profile_history', COUNT(*) FROM site_profile_history
  UNION ALL
  SELECT 'site_status_history', COUNT(*) FROM site_status_history
  UNION ALL
  SELECT 'site_occupancy', COUNT(*) FROM site_occupancy;
"
```

### Occupancy generation logic

```
For each site for each day in 2-year window:
  - 3% chance → skip row (simulate missing data)
  - Before active_date → occupancy = 0
  - After inactive_date → occupancy = 0
  - After colocation date (SITE_009) → occupancy = 0
  - During renovation (SITE_010 Jul-Oct 2024) → 2-5% of capacity
  - Weekday → 60-90% of capacity (numpy normal distribution)
  - Weekend → 5-15% of capacity (numpy normal distribution)
  
  Capacity resolved historically:
    - Query site_profile_history for capacity changes
    - Use old_value for dates before the change
    - Use current value for dates after
```

---

## Module 2 — CSV Generator

### Files

| File | Purpose |
|---|---|
| `csv_config.py` | Units by country, cost ranges, base consumption rates |
| `generator.py` | Monthly invoice row generation with occupancy + seasonal factors |
| `exporter.py` | Writes rows to CSV via pandas |
| `run_generator.py` | Entry point — loops 24 months, generates + exports |

### How to run

```bash
docker exec -it api_simulator python csv_generator/run_generator.py
```

### Generation logic

```
For each month in 2-year range:
  For each site:
    1. Check site active for this billing period
    2. 2% chance → skip (simulate missing invoice)
    3. Query site_occupancy → calculate avg occupancy factor for month
    4. Look up seasonal factor by climate zone and month
    5. For each energy type the site uses:
       base = site_sqm × random rate (BASE_CONSUMPTION_PER_SQM)
       adjusted = base × occupancy_factor × seasonal_factor × noise (±5%)
       converted to country-specific unit
       cost = consumption × unit_rate (with ±0.5% tariff noise)
    6. Append row to monthly CSV
```

### Seasonal factors by climate zone

| Climate Zone | Countries | Winter heating | Summer cooling |
|---|---|---|---|
| northern_temperate | UK, France, Germany, USA, Canada, Japan | High gas/district | Moderate electricity |
| hot_arid | UAE | N/A | Very high electricity year-round |
| tropical | Singapore | N/A | Slight electricity increase |
| southern_temperate | Australia, Brazil | Reversed — peak Jul-Aug | Reversed — peak Jan-Feb |

---

## Module 3 — API Simulator

### Files

| File | Purpose |
|---|---|
| `api_config.py` | Consumption ranges, temperature ranges, anomaly config, usage weights |
| `generator.py` | Minute-level generation (invoice-aligned, anomaly simulation) |
| `storage.py` | Parquet partition read/write/append operations |
| `backfill.py` | Vectorized 2-year historical backfill |
| `scheduler.py` | APScheduler hourly append job |
| `api.py` | FastAPI route definitions |
| `main.py` | Startup orchestrator |

### Startup sequence

```
1. Load sites.json
2. Connect to PostgreSQL
3. Run backfill (skips existing partitions — idempotent)
4. Start APScheduler hourly job
5. Run hourly job immediately (don't wait 60 minutes)
6. Start uvicorn / FastAPI
```

### Backfill performance

- Vectorized numpy operations — no Python minute loop
- Full 2-year backfill for 10 sites completes in ~7-10 seconds
- Occupancy factors pre-fetched once per month per site (not per minute)
- Invoice targets pre-fetched once per month per site

### Parquet partition structure

```
output/parquet/
├── SITE_001/
│   ├── year=2024/
│   │   ├── month=01/
│   │   │   └── data.parquet    # 44,640 rows
│   │   ├── month=02/
│   │   │   └── data.parquet    # 40,320 rows (28 days)
│   ...
│   └── year=2026/
│       └── month=04/
│           └── data.parquet    # grows every hour
```

### Hourly scheduler

- Runs every 60 minutes via APScheduler BackgroundScheduler
- Generates 60 rows per site (1 per minute) = 600 rows per run
- Appends to current month partition with deduplication
- Uses fresh DB connection per run (thread safe)
- `coalesce=True` — if missed runs, executes once not multiple times
- `max_instances=1` — prevents overlapping runs

---

## Shared Configuration

### `config/shared_config.py`

Shared between `csv_generator` and `api_simulator`:
- `CLIMATE_ZONE_BY_COUNTRY` — maps each country to a climate zone
- `SEASONAL_FACTORS` — monthly multipliers per energy type per climate zone

### `config/sites.json`

Source of truth for all 10 site definitions. Used by all 3 modules.

Key fields per site:
- `site_id`, `site_name`, `country`, `city`, `timezone`
- `latitude`, `longitude`, `site_sqm`, `site_capacity`
- `active_date`, `inactive_date`, `status`
- `billing_cycle` — calendar or mid_month
- `energy_types` — list of energy sources the site uses
- `lifecycle_event` — optional colocation or inactive_period event

---

## Running the Platform

### First time setup

```bash
cd ~/projects/global-energy-platform/fake_data_platform

# start all containers
docker compose up -d --build

# verify all containers running
docker compose ps

# watch startup logs (backfill takes ~10 seconds)
docker compose logs api_simulator --follow
```

### Run database seeder manually

```bash
# truncate all tables first (fresh start)
docker exec -it postgres psql -U energy_user -d energy_fake \
  -c "TRUNCATE TABLE site_occupancy, site_profile_history, site_status_history, site_profile CASCADE;"

# run full seeder
docker exec -it api_simulator python db_seeder/run_seeder.py
```

### Run CSV generator manually

```bash
docker exec -it api_simulator python csv_generator/run_generator.py
```

### Verify outputs

```bash
# check CSV files
ls ~/projects/global-energy-platform/fake_data_platform/output/csv/

# check parquet files
ls ~/projects/global-energy-platform/fake_data_platform/output/parquet/

# check a specific parquet partition
ls ~/projects/global-energy-platform/fake_data_platform/output/parquet/SITE_001/year=2024/month=01/
```

### Access pgAdmin

```
URL: http://<WSL2_IP>:5050
Email: value from PGADMIN_EMAIL in .env
Password: value from PGADMIN_PASSWORD in .env
```

---

## API Reference

### Base URL

```
http://<WSL2_IP>:8000
```

### Endpoints

**`GET /health`**

Health check.

```json
{ "status": "ok", "sites_loaded": 10 }
```

---

**`GET /sites`**

Returns all 10 site metadata records.

```json
[
  {
    "site_id": "SITE_001",
    "site_name": "London Headquarters",
    "country": "United Kingdom",
    "city": "London",
    "timezone": "Europe/London",
    "status": "active",
    "active_date": "2019-06-01",
    "inactive_date": null,
    "site_sqm": 6200,
    "site_capacity": 450,
    "billing_cycle": "calendar",
    "energy_types": ["electricity", "natural_gas", "district_energy"]
  }
]
```

---

**`GET /site/{site_id}/consumption`**

Query params:
- `from_ts` — start unix timestamp (required)
- `to_ts` — end unix timestamp (required)
- Max range: 90 days

```json
{
  "site_id": "SITE_001",
  "from_ts": 1704067200,
  "to_ts": 1704153600,
  "rows": 1440,
  "data": [
    {
      "timestamp": 1704067200,
      "heating": 1.2341,
      "cooling": 0.4521,
      "lighting": 0.6712,
      "ventilation": 0.3401,
      "ups": 0.1234,
      "it": 0.8921,
      "restaurant": 0.0512
    }
  ]
}
```

---

**`GET /site/{site_id}/temperature`**

Query params:
- `from_ts` — start unix timestamp (required)
- `to_ts` — end unix timestamp (required)
- Max range: 90 days

```json
{
  "site_id": "SITE_001",
  "from_ts": 1704067200,
  "to_ts": 1704153600,
  "rows": 1440,
  "data": [
    {
      "timestamp": 1704067200,
      "avg_outside_temp": 4.21,
      "degree_day_cooling": 0.0,
      "degree_day_heating": 13.79,
      "reference_temp": 18.0
    }
  ]
}
```

### Error responses

| HTTP Code | Scenario |
|---|---|
| 404 | site_id not found |
| 400 | from_ts >= to_ts |
| 400 | Range exceeds 90 days |

---

## Data Quality & Anomaly Rules

### Occupancy data (~97% accurate)

| Issue | Frequency | Simulated by |
|---|---|---|
| Missing daily record | ~3% of days | Random skip in seed_occupancy.py |
| Occupancy during renovation | Jul-Oct 2024 (SITE_010) | 2-5% of capacity |
| Zero occupancy after colocation | After Apr 2025 (SITE_009) | Lifecycle event check |

### Invoice data (~95% accurate)

| Issue | Frequency | Simulated by |
|---|---|---|
| Missing monthly invoice | ~2% of site-months | Random in generator.py |
| Mid-month billing cycle | SITE_002, SITE_005 | billing_cycle = mid_month |
| Unit rate variance | Every month | ±0.5% noise on unit rate |

### API vs Invoice alignment (anomaly simulation)

| Scenario | Probability | API / Invoice ratio | Flag |
|---|---|---|---|
| Normal | 90% | 0.92 — 0.99 | None — expected |
| High anomaly | 5% | 1.02 — 1.10 | ANOMALY [HIGH] |
| Low anomaly | 5% | 0.50 — 0.70 | ANOMALY [LOW] |

Anomalies are logged as WARNING in `api_simulator` logs and should be detected by the downstream Global Energy Platform validation pipeline.

---

## Site Reference

| Site ID | Name | Country | Status | Billing | Special |
|---|---|---|---|---|---|
| SITE_001 | London Headquarters | UK | Active | Calendar | — |
| SITE_002 | Paris Innovation Hub | France | Active | Mid-month | — |
| SITE_003 | New York Data Center | USA | Active | Calendar | — |
| SITE_004 | Tokyo Operations Center | Japan | Active | Calendar | Received SITE_009 |
| SITE_005 | Sydney Regional Office | Australia | Active | Mid-month | Reversed seasons |
| SITE_006 | Dubai Logistics Hub | UAE | Active | Calendar | Hot-arid climate |
| SITE_007 | Toronto Corporate Office | Canada | Active | Calendar | — |
| SITE_008 | Berlin Tech Campus | Germany | Active | Calendar | — |
| SITE_009 | Singapore Asia Gateway | Singapore | Colocated | Calendar | Merged into SITE_004 Apr 2025 |
| SITE_010 | São Paulo South America Hub | Brazil | Active | Calendar | Inactive Jul-Oct 2024 |

---

## Cross-Source Consistency Rules

This is the core validation principle driving the Global Energy Platform's anomaly detection layer.

### API vs Invoice

The sum of all 7 API usage type values (electricity) for a billing period should approximately match the invoice consumption for electricity.

```
API monthly aggregate (kWh) ≈ Invoice electricity consumption (kWh)
```

| Relationship | Meaning | Action |
|---|---|---|
| API ≈ Invoice (±8%) | Normal sensor behaviour | No action |
| API slightly lower (8-20%) | Acceptable — meter lag, rounding | No action |
| API much lower (>30%) | Sensor outage or data loss | Flag as LOW anomaly |
| API higher than invoice | Overcounting or billing error | Flag as HIGH anomaly |

### Why mid-month billing creates complexity

SITE_002 and SITE_005 invoice from the 15th of one month to the 14th of the next. When the Global Energy Platform aggregates by calendar month, these sites' invoice data must be realigned:

```
Invoice covers: Jan 15 → Feb 14
Calendar month: Jan 1  → Jan 31  (partial overlap)
Calendar month: Feb 1  → Feb 28  (partial overlap)
```

This requires pro-rata allocation in the Silver layer transformations.

### Occupancy → Consumption relationship

Higher occupancy should generally correlate with higher consumption:
- More people → more heating/cooling demand
- More people → more lighting, ventilation, restaurant usage
- IT and UPS are relatively constant regardless of occupancy

This relationship can be validated in the Gold layer analytics.