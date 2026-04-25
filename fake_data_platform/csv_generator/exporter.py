# csv_generator/exporter.py
#
# Handles writing generated invoice rows to monthly CSV files.
# One CSV file per month covering all sites and all energy types.
# Output path: fake_data_platform/output/csv/invoices_YYYY_MM.csv

import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# output directory relative to fake_data_platform root
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR  = os.path.join(BASE_DIR, 'output', 'csv')

# column order matches the data specification
CSV_COLUMNS = [
    "site_id",
    "site_name",
    "billing_period_from",
    "billing_period_to",
    "consumption_type",
    "consumption",
    "consumption_unit",
    "consumption_cost",
    "consumption_cost_unit",
    "cost_per_consumption_unit",
]


def get_output_path(year, month):
    """
    Returns the full file path for a monthly invoice CSV.
    Creates the output directory if it does not exist.

    Format: output/csv/invoices_YYYY_MM.csv
    Example: output/csv/invoices_2024_01.csv
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"invoices_{year}_{month:02d}.csv"
    return os.path.join(OUTPUT_DIR, filename)


def export_month(rows, year, month):
    """
    Writes all invoice rows for a given month to a CSV file.

    Each row is a dict produced by generator.generate_month().
    Columns are written in the order defined by CSV_COLUMNS.

    Skips writing if rows list is empty — this happens when all sites
    had missing invoices or were inactive for that month.

    Overwrites any existing file for the same year/month —
    allows clean re-runs without stale data accumulating.
    """
    if not rows:
        logger.warning(f"No rows to export for {year}-{month:02d} — skipping file")
        return

    output_path = get_output_path(year, month)

    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"{year}-{month:02d} → {len(df)} rows exported to {output_path}")