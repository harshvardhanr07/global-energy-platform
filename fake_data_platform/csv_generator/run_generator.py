# csv_generator/run_generator.py
#
# Entry point for the CSV invoice generator.
# Loops through every month in the 2-year window,
# generates invoice rows for all 10 sites per month,
# and exports one CSV file per month.
#
# Output: fake_data_platform/output/csv/invoices_YYYY_MM.csv

import json
import logging
import psycopg2
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

from generator import generate_month
from exporter import export_month

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sites_path = os.path.join(BASE_DIR, 'config', 'sites.json')


def load_sites():
    """
    Loads the 10 site definitions from config/sites.json.
    Raises on failure — site data is required for all downstream functions.
    """
    try:
        with open(sites_path) as file:
            data = json.load(file)
        return data
    except Exception as e:
        logger.error(f"Could not read sites.json file: {e}")
        raise


def get_db_connection():
    """
    Creates and returns a psycopg2 connection using env vars.
    Occupancy data is queried per month to calculate occupancy factors.
    """
    return psycopg2.connect(
        host     = os.getenv("POSTGRES_HOST"),
        dbname   = os.getenv("POSTGRES_DB"),
        user     = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
        port     = os.getenv("POSTGRES_PORT")
    )


def run(cursor, sites):
    """
    Main generation loop — iterates month by month over the 2-year window.

    For each month:
    - Generates invoice rows for all 10 sites
    - Flattens rows from all sites into a single list
    - Exports to one CSV file per month

    Collects all rows across all months for summary stats at the end.
    """
    start_date   = date.today() - timedelta(days=730)
    end_date     = date.today()
    current_date = start_date
    all_rows     = []  # accumulates all rows for final summary

    while current_date <= end_date:
        current_year  = current_date.year
        current_month = current_date.month

        # flatten rows from all sites into a single list for this month
        rows = [
            row
            for site in sites
            for row in generate_month(site, current_year, current_month, cursor)
        ]

        export_month(rows, current_year, current_month)
        all_rows.extend(rows)

        logger.info(f"Processed {current_year}-{current_month:02d} → {len(rows)} rows")

        current_date += relativedelta(months=1)

    # summary stats using pandas
    if all_rows:
        summary = pd.DataFrame(all_rows)
        logger.info(f"Total rows generated: {len(summary)}")
        logger.info(f"\nRows per site:\n{summary.groupby('site_id').size().to_string()}")
        logger.info(f"\nRows per energy type:\n{summary.groupby('consumption_type').size().to_string()}")


def main():
    """
    Entry point — loads sites, connects to DB, runs the generator.
    DB connection needed to query occupancy factors per month.
    """
    try:
        sites = load_sites()
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                run(cursor, sites)
        logger.info("CSV generation completed successfully")
    except Exception as e:
        logger.error(f"CSV generation failed: {e}")
        raise


if __name__ == "__main__":
    main()