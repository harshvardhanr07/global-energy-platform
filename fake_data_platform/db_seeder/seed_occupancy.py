# db_seeder/seed_occupancy.py
#
# Generates and inserts daily occupancy records for all 10 sites
# covering a 2-year rolling window from today.
#
# Key behaviours:
#   - Capacity is resolved historically using site_profile_history
#   - Occupancy reflects weekday/weekend patterns
#   - Lifecycle events (colocation, renovation) reduce occupancy accordingly
#   - ~3% of rows are randomly skipped to simulate missing data

import json
import os
import logging
import numpy as np
from schema import get_connection
from datetime import datetime, date, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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


def get_site_profile_history(cursor):
    """
    Fetches all historical site_capacity changes from site_profile_history.
    Pre-fetched once and passed to generate_occupancy() to avoid
    running one query per day per site (would be 7,300+ queries).

    Returns list of tuples: (site_id, old_value, new_value, changed_on)
    """
    cursor.execute("""
        SELECT site_id, old_value, new_value, changed_on 
        FROM site_profile_history 
        WHERE change_field = 'site_capacity'
        ORDER BY site_id, changed_on
    """)
    return cursor.fetchall()


def get_capacity(site, history, current_date):
    """
    Resolves the correct site capacity for a given date using history.

    Walks through the pre-fetched capacity history for this site.
    If a change occurred AFTER current_date, the old_value was in effect.
    If no such change exists, the current capacity from site_profile is used.

    Example:
        SITE_001 capacity changed to 450 on 2023-03-10.
        For any date before 2023-03-10 → returns 400 (old_value).
        For any date on or after 2023-03-10 → returns 450 (current).
    """
    capacity = next(
        (int(row[1]) for row in history if current_date < row[3].date()),
        site["site_capacity"]  # fallback to current capacity
    )
    return capacity


def get_occupancy(site, current_date, site_capacity):
    """
    Returns the occupancy headcount for a site on a given date.

    Priority order of checks:
    1. Colocated site → 0 after effective colocation date (SITE_009)
    2. Permanently inactive site → 0 after inactive_date
    3. Before site opened → 0 before active_date
    4. Renovation / inactive period → skeleton crew (2–5% of capacity)
    5. Normal weekday → 60–90% of capacity
    6. Normal weekend → 5–15% of capacity
    """
    lifecycle = site.get("lifecycle_event", {})
    inactive_from_str = lifecycle.get("inactive_from")
    inactive_to_str   = lifecycle.get("inactive_to")

    # parse renovation window dates if present
    inactive_from = (
        datetime.strptime(inactive_from_str, "%Y-%m-%d").date()
        if inactive_from_str else None
    )
    inactive_to = (
        datetime.strptime(inactive_to_str, "%Y-%m-%d").date()
        if inactive_to_str else None
    )

    colocated_date_str = lifecycle.get("effective_date")
    inactive_date = (
        datetime.strptime(site["inactive_date"], "%Y-%m-%d").date()
        if site["inactive_date"] else None
    )

    # 1. colocated site — no occupancy after merge date (SITE_009 → SITE_004)
    if colocated_date_str:
        colocated_date = datetime.strptime(colocated_date_str, "%Y-%m-%d").date()
        if current_date >= colocated_date:
            return 0

    # 2. permanently inactive — site closed
    if inactive_date is not None and current_date >= inactive_date:
        return 0

    # 3. site not yet opened
    if current_date < datetime.strptime(site["active_date"], "%Y-%m-%d").date():
        return 0

    # 4. temporary renovation/inactive period — security staff only (SITE_010)
    if (lifecycle.get("event_type") == "inactive_period"
            and inactive_from <= current_date <= inactive_to):
        return int(site_capacity * np.random.normal(loc=0.035, scale=0.008))  # renovation

    # 5 & 6. normal operations — weekday vs weekend pattern
    if current_date.weekday() < 5:  # Monday=0, Friday=4
        return int(site_capacity * np.random.normal(loc=0.75,  scale=0.08))   # weekday
    else:
        return int(site_capacity * np.random.normal(loc=0.10,  scale=0.03))   # weekend


def generate_occupancy(site, history) -> list:
    """
    Generates daily occupancy rows for a single site over the 2-year window.

    Randomly skips ~3% of days to simulate missing sensor/attendance data —
    a realistic imperfection that the downstream pipeline must handle.

    Returns list of tuples: (site_id, site_capacity, date, occupancy)
    """
    rows = []
    start_date   = date.today() - timedelta(days=730)
    end_date     = date.today()
    current_date = start_date

    while current_date <= end_date:

        # simulate ~3% missing data — no row inserted for this day
        if np.random.random() < 0.03:
            current_date += timedelta(days=1)
            continue

        site_capacity = get_capacity(site, history, current_date)
        occupancy     = get_occupancy(site, current_date, site_capacity)

        rows.append((site["site_id"], site_capacity, current_date, occupancy))
        current_date += timedelta(days=1)

    return rows


def seed_occupancy(cursor, sites, history):
    """
    Inserts generated occupancy rows for all sites into site_occupancy table.

    Uses ON CONFLICT DO NOTHING so re-running is safe — existing rows are
    not overwritten, preventing duplicate data on multiple seeder runs.
    """
    for site in sites:
        # filter pre-fetched history to only this site's capacity changes
        site_history = [row for row in history if row[0] == site["site_id"]]
        rows         = generate_occupancy(site, site_history)

        for row in rows:
            cursor.execute("""
                INSERT INTO site_occupancy (site_id, site_capacity, date, occupancy)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (site_id, date) DO NOTHING
            """, row)

        logger.info(f"{site['site_id']} → {len(rows)} occupancy rows inserted")


def main():
    """
    Entry point for occupancy seeding.
    Connects to DB, pre-fetches capacity history, then seeds all sites.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                site_data         = load_sites()
                site_history_data = get_site_profile_history(cursor)
                seed_occupancy(cursor, site_data, site_history_data)
            conn.commit()
        logger.info("Occupancy seeding completed successfully")
    except Exception as e:
        logger.error(f"Occupancy seeding failed: {e}")
        raise


if __name__ == "__main__":
    main()