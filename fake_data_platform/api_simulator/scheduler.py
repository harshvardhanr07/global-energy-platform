# api_simulator/scheduler.py
#
# Runs a background hourly job that generates the last 60 minutes
# of sensor data for all 10 sites and appends to current month's partition.
#
# Architecture:
#   Main thread       → FastAPI serving API requests
#   Background thread → APScheduler running hourly job
#
# Thread safety:
#   DB connections cannot be shared across threads safely.
#   A cursor_factory function is passed instead of a cursor directly —
#   each hourly job run creates its own fresh DB connection and closes it.
#
# Hourly job flow:
#   1. Get current hour's unix timestamp range
#   2. For each site → get occupancy factor + invoice target
#   3. Generate 60 minute rows per site
#   4. Append to current month's Parquet partition via storage.py

import os
import sys
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from api_config import (
    TIME_OF_DAY_FACTORS,
    CONSUMPTION_RANGES,
    USAGE_WEIGHTS,
    CLIMATE_ZONE_BY_COUNTRY,
    SEASONAL_FACTORS,
    TEMPERATURE_RANGES,
)
from generator import get_target_per_minute, generate_minute
from storage import append_to_partition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Current Hour Range ─────────────────────────────────────────────────────

def get_current_hour_range():
    """
    Returns (start_ts, end_ts) unix timestamps for the current hour.

    Floors current time to the hour boundary:
        Current time: 2026-04-26 14:35:22 UTC
        start_ts    : 2026-04-26 14:00:00 UTC → 1745672400
        end_ts      : 2026-04-26 14:59:00 UTC → 1745675940

    end_ts is last MINUTE of the hour (not last second) because
    data is generated at minute granularity (step=60).
    """
    now      = datetime.now(tz=timezone.utc)

    # floor to hour boundary
    start_dt = now.replace(minute=0, second=0, microsecond=0)
    start_ts = int(start_dt.timestamp())

    # last minute of the hour = start + 59 minutes
    end_ts   = start_ts + (59 * 60)

    return start_ts, end_ts


# ── Occupancy Factor Today ─────────────────────────────────────────────────

def get_occupancy_factor_today(site_id, site_capacity, cursor):
    """
    Queries site_occupancy for today's occupancy factor for a site.

    Returns factor = occupancy / site_capacity capped between 0.1 and 1.0.
    Returns 0.7 as default if no record found for today.

    Called once per site per hourly run — not once per minute.
    Uses CURRENT_DATE in SQL to always target today regardless of timezone.
    """
    try:
        cursor.execute("""
            SELECT occupancy, site_capacity
            FROM site_occupancy
            WHERE site_id = %s
              AND date = CURRENT_DATE
        """, (site_id,))

        row = cursor.fetchone()

        if row is None or row[0] is None or row[1] is None:
            logger.debug(f"No occupancy record for {site_id} today — using default 0.7")
            return 0.7

        factor = float(row[0]) / float(row[1])
        return round(max(0.1, min(1.0, factor)), 2)

    except Exception as e:
        logger.warning(f"Occupancy query failed for {site_id}: {e} — using default 0.7")
        return 0.7


# ── Hour Generator ─────────────────────────────────────────────────────────

def generate_hour(site, start_ts, end_ts, occupancy_factor, target_per_minute):
    """
    Generates exactly 60 minute rows for one site for one hour.

    Uses generate_minute() from generator.py for each timestamp —
    keeps consumption logic centralised in one place.

    target_per_minute is passed through from the monthly invoice target
    so hourly data stays aligned with the invoice aggregate.

    Returns a pandas DataFrame with 60 rows and PARQUET_COLUMNS schema.
    """
    # generate all 60 minute timestamps for this hour
    timestamps = np.arange(start_ts, end_ts + 60, 60, dtype=np.int64)

    rows = [
        generate_minute(site, int(ts), occupancy_factor, target_per_minute)
        for ts in timestamps
    ]

    return pd.DataFrame(rows)


# ── Hourly Job ─────────────────────────────────────────────────────────────

def run_hourly_job(sites, cursor_factory):
    """
    Main job function executed every hour by APScheduler.

    Creates a fresh DB connection per run — required for thread safety.
    APScheduler runs this in a background thread and DB connections
    cannot be safely shared across threads.

    Steps per site:
    1. Get current hour unix timestamp range
    2. Get today's occupancy factor from DB
    3. Get monthly invoice target → target_per_minute
    4. Generate 60 minute rows
    5. Append to current month's Parquet partition

    Logs total rows appended and wall time at the end.
    Catches per-site exceptions so one failed site doesn't stop others.
    """
    import time
    start_time = time.time()

    now        = datetime.now(tz=timezone.utc)
    year       = now.year
    month      = now.month
    start_ts, end_ts = get_current_hour_range()

    logger.info(
        f"Hourly job started — "
        f"{now.strftime('%Y-%m-%d %H:%M UTC')} "
        f"| range: {start_ts} → {end_ts}"
    )

    # fresh DB connection for this thread
    conn   = cursor_factory()
    cursor = conn.cursor()

    total_rows = 0

    for site in sites:
        try:
            site_id      = site["site_id"]
            site_capacity = site["site_capacity"]

            # get occupancy factor for today
            occupancy_factor = get_occupancy_factor_today(
                site_id, site_capacity, cursor
            )

            # get invoice target for current month
            target_per_minute, scenario = get_target_per_minute(
                site_id, year, month
            )

            # generate 60 rows for this hour
            hour_df = generate_hour(
                site, start_ts, end_ts,
                occupancy_factor, target_per_minute
            )

            # append to current month partition
            append_to_partition(hour_df, site_id, year, month)

            total_rows += len(hour_df)

            logger.info(
                f"{site_id} → {len(hour_df)} rows appended "
                f"| occupancy: {occupancy_factor} "
                f"| scenario: {scenario or 'free-range'}"
            )

        except Exception as e:
            logger.error(f"Hourly job failed for {site['site_id']}: {e}")
            continue

    # close connection after all sites processed
    cursor.close()
    conn.close()

    elapsed = round(time.time() - start_time, 2)
    logger.info(
        f"Hourly job complete — "
        f"{total_rows} rows appended across {len(sites)} sites | "
        f"{elapsed}s"
    )


# ── Scheduler ──────────────────────────────────────────────────────────────

def start_scheduler(sites, cursor_factory):
    """
    Sets up and starts the APScheduler background scheduler.

    Runs run_hourly_job() every 60 minutes as an interval job.
    BackgroundScheduler runs in a daemon thread — does not block
    the main thread so FastAPI can start and serve requests normally.

    Also runs the job immediately on startup so the current hour's
    data is generated without waiting 60 minutes for first run.

    Returns the scheduler instance so main.py can call
    scheduler.shutdown() on application exit for clean teardown.
    """
    scheduler = BackgroundScheduler(
        job_defaults={
            "coalesce":      True,   # if job missed, run once not multiple times
            "max_instances": 1,      # prevent overlapping runs
        }
    )

    # add hourly interval job
    scheduler.add_job(
        func     = run_hourly_job,
        trigger  = "interval",
        minutes  = 60,
        args     = [sites, cursor_factory],
        id       = "hourly_sensor_data",
        name     = "Hourly Sensor Data Generator",
    )

    scheduler.start()
    logger.info("Scheduler started — hourly sensor data job active")

    # run immediately on startup — don't wait 60 minutes for first data
    logger.info("Running initial hourly job on startup...")
    run_hourly_job(sites, cursor_factory)

    return scheduler