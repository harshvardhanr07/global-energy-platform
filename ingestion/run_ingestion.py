# run_ingestion.py
# Main entrypoint for the Bronze ingestion pipeline.
# Wires all ingestors together and prints a summary on completion.
#
# Usage inside Docker:
#   python run_ingestion.py
#
# Environment variables are loaded from .env via docker-compose

import os
import sys
import psycopg2
from base.spark_session import get_spark
from base.base_ingestor import BronzeConfig, IngestionResult
from jobs.csv_ingestor import CsvIngestor
from jobs.api_ingestor import ApiIngestor
from jobs.db_ingestor import DbIngestor
from jobs.timeseries_api_ingestor import TimeSeriesApiIngestor

# ── Environment variables ────────────────────────────────────────────────────
BRONZE_ROOT  = os.getenv("BRONZE_ROOT",   "/data/bronze")
CSV_DIR      = os.getenv("CSV_INPUT_DIR", "/data/raw/csv")
API_BASE_URL = os.getenv("API_BASE_URL",  "http://api_simulator:8000")
DB_JDBC_URL  = os.getenv("DB_JDBC_URL",   "jdbc:postgresql://postgres:5432/energy_fake")
DB_USER      = os.getenv("DB_USER",       "energy_user")
DB_PASSWORD  = os.getenv("DB_PASSWORD",   "energy_pass")
DB_HOST      = os.getenv("DB_HOST",       "postgres")
DB_PORT      = os.getenv("DB_PORT",       "5432")
DB_NAME      = os.getenv("DB_NAME",       "energy_fake")

# All site IDs available in the platform
SITE_IDS = [
    "SITE_001", "SITE_002", "SITE_003", "SITE_004", "SITE_005",
    "SITE_006", "SITE_007", "SITE_008", "SITE_009", "SITE_010",
]


def run_csv(spark) -> list:
    """Ingest invoice CSV files produced by fake_data_platform csv_generator."""
    tables = {
        "invoices": f"{CSV_DIR}/invoices_*.csv",
    }
    results = []
    for table_name, path in tables.items():
        config = BronzeConfig(bronze_root=BRONZE_ROOT, source_name="csv", table_name=table_name)
        ingestor = CsvIngestor(spark, config, csv_path=path)
        results.append(ingestor.run())
    return results


def run_api(spark) -> list:
    """Ingest static API endpoints from the fake_data_platform API simulator."""
    endpoints = {
        "sites": "/sites",
    }
    results = []
    for table_name, endpoint in endpoints.items():
        config = BronzeConfig(bronze_root=BRONZE_ROOT, source_name="api", table_name=table_name)
        ingestor = ApiIngestor(spark, config, base_url=API_BASE_URL, endpoint=endpoint)
        results.append(ingestor.run())
    return results


def run_db(spark) -> list:
    """Ingest PostgreSQL tables seeded by fake_data_platform db_seeder."""
    tables = {
        "site_profile":         "public.site_profile",
        "site_occupancy":       "public.site_occupancy",
        "site_profile_history": "public.site_profile_history",
        "site_status_history":  "public.site_status_history",
    }
    results = []
    for table_name, db_table in tables.items():
        config = BronzeConfig(bronze_root=BRONZE_ROOT, source_name="db", table_name=table_name)
        ingestor = DbIngestor(
            spark, config,
            jdbc_url=DB_JDBC_URL,
            db_table=db_table,
            db_user=DB_USER,
            db_password=DB_PASSWORD,
        )
        results.append(ingestor.run())
    return results


def run_timeseries_api(spark, db_conn) -> list:
    """
    Ingest per-site time series data from the API simulator.
    Uses watermark tracking for incremental monthly loads.
    Covers two endpoints: consumption and temperature.
    """
    results = []

    for endpoint_name in ["consumption", "temperature"]:
        config = BronzeConfig(
            bronze_root=BRONZE_ROOT,
            source_name="api",
            table_name=f"site_{endpoint_name}",
        )
        ingestor = TimeSeriesApiIngestor(
            spark=spark,
            config=config,
            base_url=API_BASE_URL,
            endpoint_name=endpoint_name,
            site_ids=SITE_IDS,
            db_conn=db_conn,
        )
        results.extend(ingestor.run())

    return results


def main():
    spark = get_spark("GEP-Ingestion")

    # Open a psycopg2 connection for watermark and ingestion log (timeseries only)
    db_conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    all_results: list = []

    try:
        # Run all sources — failures inside each ingestor are caught individually
        # so one bad table does not stop the rest
        all_results.extend(run_csv(spark))
        all_results.extend(run_api(spark))
        all_results.extend(run_db(spark))
        all_results.extend(run_timeseries_api(spark, db_conn))

    finally:
        spark.stop()
        db_conn.close()

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)
    failures = 0
    for r in all_results:
        status = "✓" if r.success else "✗"
        print(f"  {status}  {r.source:<6} / {r.table:<35}  {r.rows_written:>6} rows")
        if not r.success:
            print(f"      ERROR: {r.error}")
            failures += 1
    print("=" * 60)

    # Exit with error code if any ingestion failed — useful for Airflow later
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
