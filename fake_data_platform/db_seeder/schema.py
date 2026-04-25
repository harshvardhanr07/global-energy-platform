# db_seeder/schema.py
#
# Creates all 4 PostgreSQL tables for the fake data platform.
# Safe to re-run — all tables use CREATE TABLE IF NOT EXISTS.
#
# Table creation order matters due to foreign key dependencies:
#   1. site_profile (no dependencies)
#   2. site_profile_history (references site_profile)
#   3. site_status_history  (references site_profile)
#   4. site_occupancy       (references site_profile)

import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection():
    """
    Creates and returns a psycopg2 connection using env vars.
    Called with a context manager (with get_connection() as conn)
    which auto-commits on success and rolls back on exception.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT")
    )


def create_site_profile(cursor) -> None:
    """
    Creates the site_profile table — the source of truth for all site data.
    Holds current state only. Historical changes tracked in site_profile_history.

    Key constraints:
    - status restricted to known values via CHECK constraint
    - billing_cycle defaults to 'calendar' — only SITE_002 and SITE_005 use 'mid_month'
    - last_updated_on auto-set to NOW() on insert
    - idx_site_profile_status index added for frequent status-based filtering
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS site_profile (
            site_id         VARCHAR(10)     PRIMARY KEY,
            site_name       VARCHAR(255)    NOT NULL,
            status          VARCHAR(20)     NOT NULL CHECK (status IN ('active','inactive','closed','colocated')),
            active_date     DATE            NOT NULL,
            inactive_date   DATE,
            country         VARCHAR(100),
            city            VARCHAR(100),
            latitude        NUMERIC(9,6),
            longitude       NUMERIC(9,6),
            site_sqm        NUMERIC(10,2)   NOT NULL,
            site_capacity   SMALLINT        NOT NULL,
            billing_cycle   VARCHAR(20)     DEFAULT 'calendar',
            timezone        VARCHAR(50),
            last_updated_on TIMESTAMP       DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_site_profile_status ON site_profile(status);
    """)
    logger.info("site_profile table ready")


def create_site_profile_history(cursor):
    """
    Creates the site_profile_history table — full audit trail of field-level changes.
    Enables point-in-time reconstruction of any site's state.

    old_value and new_value stored as TEXT to accommodate any field type
    (strings, numbers, dates) without schema changes.

    ON DELETE CASCADE ensures history rows are removed if a site is deleted.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS site_profile_history (
            id              SERIAL          PRIMARY KEY,
            site_id         VARCHAR(10)     NOT NULL REFERENCES site_profile(site_id) ON DELETE CASCADE,
            change_field    VARCHAR(100)    NOT NULL,
            old_value       TEXT,
            new_value       TEXT,
            changed_on      TIMESTAMP       NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_site_profile_history_site ON site_profile_history(site_id);
    """)
    logger.info("site_profile_history table ready")


def create_site_status_history(cursor):
    """
    Creates the site_status_history table — tracks major lifecycle events.

    event_type restricted to known values:
    - moved         → site physically relocated
    - closed        → site permanently decommissioned
    - colocated     → site merged into another site (e.g. SITE_009 → SITE_004)
    - inactive_period → temporary closure, site later reopened (e.g. SITE_010)

    new_site_id is nullable — only populated for colocated/moved events.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS site_status_history (
            id              SERIAL      PRIMARY KEY,
            site_id         VARCHAR(10) NOT NULL REFERENCES site_profile(site_id) ON DELETE CASCADE,
            event_type      VARCHAR(20) NOT NULL CHECK (event_type IN ('moved','closed','colocated','inactive_period')),
            new_site_id     VARCHAR(10) REFERENCES site_profile(site_id),
            updated_on      TIMESTAMP   NOT NULL DEFAULT NOW()
        );
    """)
    logger.info("site_status_history table ready")


def create_site_occupancy(cursor):
    """
    Creates the site_occupancy table — daily headcount per site.
    ~97% accurate by design — ~3% of rows intentionally missing.

    UNIQUE constraint on (site_id, date) prevents duplicate daily entries
    and enables ON CONFLICT DO NOTHING in the seeder for safe re-runs.

    Composite index on (site_id, date) optimises the most common query pattern:
    filtering occupancy for a specific site over a date range.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS site_occupancy (
            id              SERIAL      PRIMARY KEY,
            site_id         VARCHAR(10) NOT NULL REFERENCES site_profile(site_id) ON DELETE CASCADE,
            site_capacity   SMALLINT    NOT NULL,
            date            DATE        NOT NULL,
            occupancy       SMALLINT,
            CONSTRAINT uq_occupancy_site_date UNIQUE (site_id, date)
        );
        CREATE INDEX IF NOT EXISTS idx_site_occupancy_site_date ON site_occupancy(site_id, date);
    """)
    logger.info("site_occupancy table ready")


def create_all_tables(cursor):
    """
    Orchestrates table creation in dependency order.
    site_profile must be created first as all other tables reference it.
    """
    create_site_profile(cursor)
    create_site_profile_history(cursor)
    create_site_status_history(cursor)
    create_site_occupancy(cursor)


def main():
    """
    Entry point for schema creation.
    All 4 tables created in a single transaction —
    if any table fails, the entire transaction rolls back.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                create_all_tables(cursor)
        logger.info("All tables created successfully")
    except Exception as e:
        logger.error(f"Schema creation failed: {e}")
        raise


if __name__ == "__main__":
    main()