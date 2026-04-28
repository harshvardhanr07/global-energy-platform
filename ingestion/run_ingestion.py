# run_ingestion.py
# Main entrypoint for a full Bronze ingestion run.
# Wires all three ingestors (CSV, API, DB) together and
# prints a summary of results at the end.
#
# Usage inside Docker:
#   python -m run_ingestion
#
# Environment variables are read from .env via docker-compose

import os
import sys
from base.spark_session import get_spark
from base.base_ingestor import BronzeConfig, IngestionResult
from jobs.csv_ingestor import CsvIngestor
from jobs.api_ingestor import ApiIngestor
from jobs.db_ingestor import DbIngestor

# ── Config from environment variables ────────────────────────────────────────
# All values come from .env via docker-compose environment block
BRONZE_ROOT  = os.getenv("BRONZE_ROOT", "/data/bronze")             # s3a:// in production
CSV_DIR      = os.getenv("CSV_INPUT_DIR", "/data/raw/csv")          # mounted from fake_data_platform
API_BASE_URL = os.getenv("API_BASE_URL", "http://api_simulator:8000")
DB_JDBC_URL  = os.getenv("DB_JDBC_URL", "jdbc:postgresql://postgres:5432/energy_fake")
DB_USER      = os.getenv("DB_USER", "energy_user")
DB_PASSWORD  = os.getenv("DB_PASSWORD", "energy_pass")


def run_csv(spark) -> list:
    # Ingest CSV files produced by fake_data_platform csv_generator
    # All invoice CSVs are flat in one folder, named invoices_YYYY_MM.csv
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
    # Ingest data from fake_data_platform API simulator endpoints
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
    # Ingest PostgreSQL tables seeded by fake_data_platform db_seeder
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
            db_password=DB_PASSWORD
        )
        results.append(ingestor.run())
    return results


def main():
    spark = get_spark()
    results = []

    # Run all three sources — failures are caught inside each ingestor
    # so one bad table doesn't stop the rest
    results.extend(run_csv(spark))
    results.extend(run_api(spark))
    results.extend(run_db(spark))

    spark.stop()

    # Print summary
    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)
    failures = 0
    for r in results:
        status = "✓" if r.success else "✗"
        print(f"  {status}  {r.source:<6} / {r.table:<25}  {r.rows_written:>6} rows")
        if not r.success:
            print(f"      ERROR: {r.error}")
            failures += 1
    print("=" * 60)

    # Exit with error code if any ingestion failed — useful for Airflow later
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()