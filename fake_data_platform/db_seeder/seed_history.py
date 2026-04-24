import json
import os
import logging
from schema import get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sites_path = os.path.join(BASE_DIR, 'config', 'sites.json')

# ── hardcoded realistic profile history changes ───────────
PROFILE_HISTORY = [
    # SITE_001 - London HQ
    ("SITE_001", "site_capacity", "400",         "450",          "2023-03-10 09:00:00"),
    ("SITE_001", "site_sqm",      "5800",         "6200",         "2023-03-10 09:00:00"),
    ("SITE_001", "status",        "inactive",     "active",       "2019-06-01 08:00:00"),

    # SITE_002 - Paris
    ("SITE_002", "site_capacity", "250",          "280",          "2023-06-15 10:30:00"),
    ("SITE_002", "billing_cycle", "calendar",     "mid_month",    "2023-01-15 08:00:00"),

    # SITE_003 - New York
    ("SITE_003", "city",          "Newark",       "New York",     "2022-08-01 12:00:00"),
    ("SITE_003", "site_capacity", "550",          "600",          "2023-09-01 09:00:00"),
    ("SITE_003", "site_sqm",      "8000",         "8500",         "2023-09-01 09:00:00"),

    # SITE_004 - Tokyo
    ("SITE_004", "site_capacity", "300",          "320",          "2023-04-01 08:00:00"),
    ("SITE_004", "site_sqm",      "3800",         "4100",         "2023-04-01 08:00:00"),

    # SITE_005 - Sydney
    ("SITE_005", "billing_cycle", "calendar",     "mid_month",    "2022-11-15 08:00:00"),
    ("SITE_005", "site_capacity", "180",          "200",          "2023-07-01 09:00:00"),

    # SITE_006 - Dubai
    ("SITE_006", "site_capacity", "480",          "520",          "2023-01-01 08:00:00"),
    ("SITE_006", "site_sqm",      "7200",         "7800",         "2023-01-01 08:00:00"),
    ("SITE_006", "status",        "inactive",     "active",       "2019-09-15 08:00:00"),

    # SITE_007 - Toronto
    ("SITE_007", "site_sqm",      "4200",         "4700",         "2023-05-10 10:00:00"),
    ("SITE_007", "site_capacity", "350",          "380",          "2023-05-10 10:00:00"),

    # SITE_008 - Berlin
    ("SITE_008", "site_capacity", "380",          "410",          "2023-08-01 09:00:00"),
    ("SITE_008", "site_sqm",      "5000",         "5300",         "2023-08-01 09:00:00"),

    # SITE_009 - Singapore
    ("SITE_009", "status",        "active",       "colocated",    "2025-04-01 00:00:00"),
    ("SITE_009", "inactive_date", "None",         "2025-03-31",   "2025-04-01 00:00:00"),

    # SITE_010 - Sao Paulo
    ("SITE_010", "status",        "active",       "inactive",     "2024-07-01 00:00:00"),
    ("SITE_010", "status",        "inactive",     "active",       "2024-11-01 00:00:00"),
    ("SITE_010", "site_capacity", "210",          "230",          "2024-11-01 00:00:00"),
]


def seed_profile_history(cursor):
    for site_id, change_field, old_value, new_value, changed_on in PROFILE_HISTORY:
        cursor.execute("""
            INSERT INTO site_profile_history (
                site_id, change_field, old_value, new_value, changed_on
            )
            VALUES (%s, %s, %s, %s, %s)
        """, (site_id, change_field, old_value, new_value, changed_on))
        logger.info(f"{site_id} | {change_field}: '{old_value}' → '{new_value}'")


def seed_status_history(cursor):
    with open(sites_path) as f:
        sites = json.load(f)

    for site in sites:
        event = site.get("lifecycle_event")
        if not event:
            continue

        event_type = event.get("event_type")

        if event_type == "colocated":
            cursor.execute("""
                INSERT INTO site_status_history (
                    site_id, event_type, new_site_id, updated_on
                )
                VALUES (%s, %s, %s, %s)
            """, (
                site["site_id"],
                "colocated",
                event.get("merged_into_site_id"),
                event.get("effective_date")
            ))
            logger.info(f"{site['site_id']} → colocated into {event.get('merged_into_site_id')}")

        elif event_type == "inactive_period":
            cursor.execute("""
                INSERT INTO site_status_history (
                    site_id, event_type, new_site_id, updated_on
                )
                VALUES (%s, %s, %s, %s)
            """, (
                site["site_id"],
                "inactive_period",
                None,
                event.get("inactive_from")
            ))
            logger.info(f"{site['site_id']} → inactive period from {event.get('inactive_from')} to {event.get('inactive_to')}")


def main():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                seed_profile_history(cursor)
                seed_status_history(cursor)
            conn.commit()
        logger.info("History data seeded successfully")
    except Exception as e:
        logger.error(f"History seeding failed: {e}")
        raise

if __name__ == "__main__":
    main()