import psycopg2
import json
import os
import logging
from schema import get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sites_path = os.path.join(BASE_DIR, 'config', 'sites.json')

def load_sites():
    try:
        with open(sites_path) as file:
            data = json.load(file)
        return data
    except Exception as e:
        logger.error(f"Could not read sites.json file: {e}")
        raise

def seed_sites(cursor, data):
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
            site['site_id'],
            site['site_name'],
            site['status'],
            site['active_date'],
            site['inactive_date'],
            site['country'],
            site['city'],
            site['latitude'],
            site['longitude'],
            site['site_sqm'],
            site['site_capacity'],
            site['billing_cycle'],
            site['timezone']
        ))
        logger.info(f"{site['site_id']} - {site['site_name']} inserted successfully")
    

def main():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                site_data = load_sites()
                seed_sites(cursor, site_data)
            conn.commit()
        logger.info("All sites data has been successfully loaded")
    except Exception as e:
        logger.error(f"Site Data is not loaded into site_profile table: {e}")
        raise

if __name__ == "__main__":
    main()