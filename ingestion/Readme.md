# Ingestion Layer — Global Energy Platform

Bronze layer ingestion pipeline. Reads raw data from three source systems in `fake_data_platform` and writes partitioned Parquet to AWS S3.

---

## Result

```
✓  csv  / invoices                 611 rows  →  s3a://gep-datalake-dev/bronze/csv/invoices/
✓  api  / sites                     10 rows  →  s3a://gep-datalake-dev/bronze/api/sites/
✓  db   / site_profile              10 rows  →  s3a://gep-datalake-dev/bronze/db/site_profile/
✓  db   / site_occupancy          7091 rows  →  s3a://gep-datalake-dev/bronze/db/site_occupancy/
✓  db   / site_profile_history      24 rows  →  s3a://gep-datalake-dev/bronze/db/site_profile_history/
✓  db   / site_status_history        2 rows  →  s3a://gep-datalake-dev/bronze/db/site_status_history/
```

---

## Folder Structure

```
ingestion/
├── base/
│   ├── __init__.py
│   ├── base_ingestor.py       # IngestionResult, BronzeConfig, BaseIngestor
│   └── spark_session.py       # SparkSession factory (local mode + S3A)
├── jobs/
│   ├── __init__.py
│   ├── csv_ingestor.py        # reads invoices_*.csv → Bronze Parquet
│   ├── api_ingestor.py        # paginates /sites API → Bronze Parquet
│   └── db_ingestor.py         # JDBC reads 4 PG tables → Bronze Parquet
├── tests/
│   └── __init__.py
├── __init__.py
├── run_ingestion.py           # entrypoint — wires all 3 sources
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env                       # not committed — see .env section below
```

---

## How It Works

Every ingestor follows the same pattern defined in `base_ingestor.py`:

```
run()
  → extract()        # source-specific read (CSV / API / JDBC)
  → _add_metadata()  # adds _ingested_at, _source, ingestion_date
  → .write.parquet() # partitioned Parquet write to S3
  → return IngestionResult
```

Three metadata columns are added to every Bronze table:

| Column | Value | Purpose |
|---|---|---|
| `_ingested_at` | UTC ISO timestamp | Audit trail |
| `_source` | csv / api / db | Data lineage |
| `ingestion_date` | YYYY-MM-DD | Parquet partition key |

---

## Sources

### CSV — `jobs/csv_ingestor.py`
Reads monthly invoice CSVs from `fake_data_platform/output/csv/` mounted via Docker volume.

| Table | Glob |
|---|---|
| `invoices` | `CSV_INPUT_DIR/invoices_*.csv` |

- `infer_schema=False` — all columns land as strings at Bronze; Silver casts types
- `multiLine=True` — handles values with line breaks
- `escape='"'` — handles quoted fields with commas

---

### API — `jobs/api_ingestor.py`
Paginates FastAPI endpoints on `fake_data_platform` api_simulator.

| Table | Endpoint |
|---|---|
| `sites` | `/sites` |

- Supports both bare list `[]` and `{"data": [...]}` response shapes
- Stops when API returns empty page or page shorter than `page_size`
- Calls `raise_for_status()` — fails fast on 4xx/5xx

---

### DB — `jobs/db_ingestor.py`
Reads PostgreSQL tables via Spark JDBC from `fake_data_platform` postgres container.

| Table | DB Table |
|---|---|
| `site_profile` | `public.site_profile` |
| `site_occupancy` | `public.site_occupancy` |
| `site_profile_history` | `public.site_profile_history` |
| `site_status_history` | `public.site_status_history` |

- Supports parallel reads via `partition_column` + `lower_bound` + `upper_bound`
- PostgreSQL JDBC JAR downloaded at Spark session start via `spark.jars.packages`

---

## Environment Variables

Create `ingestion/.env` with these values (never commit this file):

```env
# Bronze output path
BRONZE_ROOT=s3a://gep-datalake-dev/bronze

# CSV source (mounted from fake_data_platform)
CSV_INPUT_DIR=/data/raw/csv

# API source
API_BASE_URL=http://api_simulator:8000

# DB source
DB_JDBC_URL=jdbc:postgresql://postgres:5432/energy_fake
DB_USER=energy_user
DB_PASSWORD=energy_pass

# AWS credentials
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=eu-west-3
S3_BUCKET=gep-datalake-dev
```

---

## Running

**Prerequisites:**
- `fake_data_platform` stack must be running (provides postgres, api_simulator, network)
- AWS credentials set in `.env`

```bash
# 1 — start fake_data_platform
cd ~/projects/global-energy-platform/fake_data_platform
docker compose up -d

# 2 — build ingestion container
cd ~/projects/global-energy-platform/ingestion
docker compose build --no-cache

# 3 — run ingestion
docker compose run --rm ingestion
```

---

## Docker

### Network
The ingestion container joins `fake_data_platform_energy_network` (external network created by fake_data_platform stack). This gives direct hostname access to `api_simulator:8000` and `postgres:5432`.

### Volumes
| Mount | Purpose |
|---|---|
| `bronze_data` → `/data/bronze` | Local Bronze fallback |
| `/home/hvsr_de/.../output/csv` → `/data/raw/csv` | CSV files from fake_data_platform |

### Key Dockerfile notes
- Base: `python:3.11-slim` + `openjdk-21-jre-headless` (17 not available in apt repos)
- Hadoop-AWS JARs downloaded at build time into PySpark's jars directory (not `/opt/spark/jars/` which doesn't exist for pip-installed PySpark)
- `COPY . /app/` — code lands at `/app/` so `python -m run_ingestion` resolves correctly

---

## Dependencies

**requirements.txt**

| Package | Version | Purpose |
|---|---|---|
| pyspark | 3.5.1 | Spark engine |
| requests | 2.32.3 | HTTP client for API pagination |
| psycopg2-binary | 2.9.9 | PostgreSQL driver |
| python-dotenv | 1.0.1 | Loads .env at startup |

**JARs (downloaded at build time)**

| JAR | Version | Purpose |
|---|---|---|
| hadoop-aws | 3.3.4 | S3A FileSystem — enables `s3a://` paths |
| aws-java-sdk-bundle | 1.12.262 | AWS SDK used by hadoop-aws |
| postgresql | 42.7.3 | JDBC driver — downloaded by Spark at runtime |

---

## Known Issues & Fixes

| Issue | Root Cause | Fix |
|---|---|---|
| `openjdk-17` not found | Not in apt repos | Switched to `openjdk-21` |
| `No FileSystem for scheme s3a` | Hadoop JARs in wrong path | Downloaded to dynamic pyspark path at build time |
| `ModuleNotFoundError: ingestion` | `COPY . /app/ingestion/` wrong dir | Changed to `COPY . /app/` |
| Network not found | Wrong network name | Used exact name from `docker network ls` |
| CSV `PATH_NOT_FOUND` | `~` not expanded in volume mount | Replaced with absolute host path |
| API 404 | Wrong endpoint paths | Checked `/openapi.json` for actual endpoints |
| `energy_db` does not exist | Wrong DB name in JDBC URL | Checked fake_data_platform `.env` → `energy_fake` |
| `BRONZE_PATH` vs `BRONZE_ROOT` | Env var name mismatch | Renamed to `BRONZE_ROOT` in `.env` |
| `bucket is null/empty` | `pathlib.Path` strips `s3a://` to `s3a:/` | Replaced with f-string path join |
| `ClassNotFoundException: postgresql` | `spark.jars.packages` missing | Added to `spark_session.py` |
| `ModuleNotFoundError` on docker run | CMD used wrong module path | Fixed CMD to `python -m run_ingestion` |
| `BRONZE_PATH` warning on startup | Unused var in docker-compose | Removed `BRONZE_PATH` line |
