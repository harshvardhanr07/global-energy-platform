import psycopg2
import sys
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT")
    )


def create_site_profile(cursor) -> None:

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS site_profile(
            site_id         VARCHAR(10)         PRIMARY KEY,
            site_name       VARCHAR(255)        NOT NULL,
            status          VARCHAR(20)         NOT NULL        CHECK (status IN ('active','inactive','closed','colocated')),
            active_date     DATE                NOT NULL,
            inactive_date   DATE,
            country         VARCHAR(100),
            city            VARCHAR(100),
            latitude        NUMERIC(9,6),
            longitude       NUMERIC(9,6),
            site_sqm        NUMERIC(10,2)       NOT NULL,
            site_capacity   SMALLINT            NOT NULL,
            billing_cycle   VARCHAR(20)         DEFAULT         'calendar',
            timezone        VARCHAR(50),
            last_updated_on TIMESTAMP           DEFAULT         NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_site_profile_status ON site_profile(status);
    """)
    logger.info("site_profile table ready")

def create_site_profile_history(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS site_profile_history(
            id              SERIAL          PRIMARY KEY,
            site_id         VARCHAR(10)     NOT NULL    REFERENCES site_profile(site_id) ON DELETE CASCADE,
            change_field    VARCHAR(100)    NOT NULL,
            old_value       TEXT,
            new_value       TEXT,
            changed_on      TIMESTAMP NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_site_profile_history_site ON site_profile_history(site_id);
    """)
    logger.info("site_profile_history table ready")

def create_site_status_history(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS site_status_history (
            id              SERIAL PRIMARY KEY,
            site_id         VARCHAR(10) NOT NULL REFERENCES site_profile(site_id) ON DELETE CASCADE,
            event_type      VARCHAR(20) NOT NULL CHECK (event_type IN ('moved','closed','colocated','inactive_period')),
            new_site_id     VARCHAR(10) REFERENCES site_profile(site_id),
            updated_on      TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)
    logger.info("site_status_history table ready")

def create_site_occupancy(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS site_occupancy (
            id              SERIAL PRIMARY KEY,
            site_id         VARCHAR(10) NOT NULL REFERENCES site_profile(site_id) ON DELETE CASCADE,
            site_capacity   SMALLINT NOT NULL,
            date            DATE NOT NULL,
            occupancy       SMALLINT,
            CONSTRAINT uq_occupancy_site_date UNIQUE (site_id, date)
        );
        CREATE INDEX IF NOT EXISTS idx_site_occupancy_site_date ON site_occupancy(site_id, date);
    """)
    logger.info("site_occupancy table ready")

def create_all_tables(cursor):
    create_site_profile(cursor)
    create_site_profile_history(cursor)
    create_site_status_history(cursor)
    create_site_occupancy(cursor)
    
def main():
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