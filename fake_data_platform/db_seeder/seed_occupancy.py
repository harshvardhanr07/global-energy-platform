import json
import os
import logging
import random
from schema import get_connection
from datetime import datetime, date, timedelta

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


def get_site_profile_history(cursor):
    cursor.execute("""
        SELECT site_id, old_value, new_value, changed_on 
        FROM site_profile_history 
        WHERE change_field = 'site_capacity'
        ORDER BY site_id, changed_on
    """)
    return cursor.fetchall()


def get_capacity(site, history, current_date):
    capacity = next(
        (int(row[1]) for row in history if current_date < row[3].date()),
        site["site_capacity"]
    )
    return capacity


def get_occupancy(site, current_date, site_capacity):
    lifecycle = site.get("lifecycle_event", {})
    inactive_from_str = lifecycle.get("inactive_from")
    inactive_to_str = lifecycle.get("inactive_to")

    inactive_from = (
        datetime.strptime(inactive_from_str, "%Y-%m-%d").date()
        if inactive_from_str else None
    )
    inactive_to = (
        datetime.strptime(inactive_to_str, "%Y-%m-%d").date()
        if inactive_to_str else None
    )

    colocated_date_str = lifecycle.get("effective_date")
    inactive_date = datetime.strptime(site["inactive_date"], "%Y-%m-%d").date() if site["inactive_date"] else None

    # colocated site — no occupancy after merge date
    if colocated_date_str:
        colocated_date = datetime.strptime(colocated_date_str, "%Y-%m-%d").date()
        if current_date >= colocated_date:
            return 0

    # site is permanently inactive
    if inactive_date is not None and current_date >= inactive_date:
        return 0

    # before site was active
    if current_date < datetime.strptime(site["active_date"], "%Y-%m-%d").date():
        return 0

    # renovation / inactive period — skeleton crew only
    if (lifecycle.get("event_type") == "inactive_period"
            and inactive_from <= current_date <= inactive_to):
        return int(site_capacity * random.uniform(0.02, 0.05))

    # normal operations
    if current_date.weekday() < 5:
        return int(site_capacity * random.uniform(0.60, 0.90))
    else:
        return int(site_capacity * random.uniform(0.05, 0.15))


def generate_occupancy(site, history) -> list:
    rows = []
    start_date = date.today() - timedelta(days=730)
    end_date = date.today()
    current_date = start_date

    while current_date <= end_date:
        # simulate ~3% missing data
        if random.random() < 0.03:
            current_date += timedelta(days=1)
            continue

        site_capacity = get_capacity(site, history, current_date)
        occupancy = get_occupancy(site, current_date, site_capacity)
        rows.append((site['site_id'], site_capacity, current_date, occupancy))
        current_date += timedelta(days=1)

    return rows


def seed_occupancy(cursor, sites, history):
    for site in sites:
        site_history = [row for row in history if row[0] == site["site_id"]]
        rows = generate_occupancy(site, site_history)
        for row in rows:
            cursor.execute("""
                INSERT INTO site_occupancy (site_id, site_capacity, date, occupancy)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (site_id, date) DO NOTHING
            """, row)
        logger.info(f"{site['site_id']} → {len(rows)} occupancy rows inserted")


def main():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                site_data = load_sites()
                site_history_data = get_site_profile_history(cursor)
                seed_occupancy(cursor, site_data, site_history_data)
            conn.commit()
        logger.info("Occupancy seeding completed successfully")
    except Exception as e:
        logger.error(f"Occupancy seeding failed: {e}")
        raise


if __name__ == "__main__":
    main()