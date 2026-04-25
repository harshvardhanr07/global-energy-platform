# db_seeder/seed_sites.py
#
# Reads site definitions from config/sites.json and inserts
# one row per site into the site_profile table.
# Safe to re-run — ON CONFLICT DO NOTHING prevents duplicates.

import psycopg2
import json
import os
import logging
from schema import get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sites_path = os.path.join(BASE_DIR, 'config', 'sites.json')


def load_sites():
    """
    Loads and returns the list of 10 site definitions from sites.json.
    Raises on failure — all downstream seeders depend on this data.
    """
    try:
        with open(sites_path) as file:
            data = json.load(file)
        return data
    except Exception as e:
        logger.error(f"Could not read sites.json file: {e}")
        raise


def seed_sites(cursor, data):
    """
    Inserts one row per site into site_profile.

    lifecycle_event is intentionally excluded — it is not a DB column.
    It is only used by seed_history.py and seed_occupancy.py.

    ON CONFLICT DO NOTHING ensures re-running the seeder does not
    create duplicate rows or overwrite existing data.
    """
    for site in data:
        cursor.execute("""
            INSERT INTO site_profile (
                site_id, site_name, status, active_date, inactive_date,
                country, city, latitude, longitude, site_sqm,
                site_capacity, billing_cycle, timezone
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (site_id) DO NOTHING
        """, (
            site["site_id"],
            site["site_name"],
            site["status"],
            site["active_date"],
            site["inactive_date"],
            site["country"],
            site["city"],
            site["latitude"],
            site["longitude"],
            site["site_sqm"],
            site["site_capacity"],
            site["billing_cycle"],
            site["timezone"],
        ))
        logger.info(f"{site['site_id']} - {site['site_name']} inserted successfully")


def main():
    """
    Entry point for site profile seeding.
    Loads sites.json and inserts all 10 sites into site_profile.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                site_data = load_sites()
                seed_sites(cursor, site_data)
            conn.commit()
        logger.info("All sites data has been successfully loaded")
    except Exception as e:
        logger.error(f"Site data not loaded into site_profile: {e}")
        raise


if __name__ == "__main__":
    main()