# api_simulator/backfill.py
#
# Generates full 2-year historical minute-level sensor data for all 10 sites.
# Runs once on startup — skips partitions that already exist.
#
# Performance strategy:
#   - Vectorized numpy operations instead of row-by-row Python loops
#   - Occupancy factors queried once per month per site (not per minute)
#   - Target per minute calculated once per month per site (not per minute)
#   - Estimated generation time: ~2-5 minutes for full 2-year backfill
#
# Output:
#   One Parquet file per site per month
#   ~44,640 rows per file (days × 24h × 60min)
#   Total: ~10.4M rows across all sites and months

import os
import sys
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timezone, date
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from api_config import (
    PARQUET_DIR,
    BACKFILL_YEARS,
    TIME_OF_DAY_FACTORS,
    CONSUMPTION_RANGES,
    USAGE_WEIGHTS,
    CLIMATE_ZONE_BY_COUNTRY,
    SEASONAL_FACTORS,
    TEMPERATURE_RANGES,
)
from generator import get_target_per_minute, get_temperature
from storage import get_partition_path, write_partition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Backfill Check ─────────────────────────────────────────────────────────

def is_backfill_needed(site_id, year, month):
    """
    Checks whether a partition already exists for the given site/year/month.

    Returns True  → partition missing, backfill needed
    Returns False → partition exists, skip to avoid overwriting historical data

    This makes backfill idempotent — safe to re-run after partial failures.
    Only current month is expected to be incomplete (hourly appends ongoing).
    """
    partition_path = get_partition_path(site_id, year, month)
    exists         = os.path.exists(partition_path)

    if exists:
        logger.debug(f"Partition exists — skipping {site_id} {year}-{month:02d}")
    return not exists


# ── Occupancy Factors ──────────────────────────────────────────────────────

def get_daily_occupancy_factors(site_id, year, month, cursor):
    """
    Queries site_occupancy table to get occupancy factor for every day
    in the given month for this site.

    Returns dict keyed by day of month: {1: 0.75, 2: 0.82, ...}

    Days with missing occupancy records (~3% by design) default to 0.7
    — a reasonable average office utilisation fallback.

    Pre-fetching all days at once avoids 44,640 individual DB queries
    that would occur if we queried inside the minute loop.
    """
    cursor.execute("""
        SELECT
            EXTRACT(DAY FROM date)::INTEGER AS day,
            occupancy::FLOAT / NULLIF(site_capacity, 0) AS occupancy_factor
        FROM site_occupancy
        WHERE site_id = %s
          AND EXTRACT(YEAR  FROM date) = %s
          AND EXTRACT(MONTH FROM date) = %s
          AND occupancy IS NOT NULL
        ORDER BY date
    """, (site_id, year, month))

    rows    = cursor.fetchall()
    factors = {row[0]: round(max(0.1, min(1.0, row[1])), 2) for row in rows}

    # fill missing days with default factor
    days_in_month = monthrange(year, month)[1]
    for day in range(1, days_in_month + 1):
        if day not in factors:
            factors[day] = 0.7  # default for missing occupancy records

    return factors


# ── Vectorized Month Generator ─────────────────────────────────────────────

def generate_month_vectorized(site, year, month, daily_factors, target_per_minute):
    """
    Generates all minute rows for one site for one month using
    vectorized numpy operations.

    Instead of looping 44,640 times, numpy arrays are used to apply
    time-of-day factors, seasonal factors, and noise across all
    timestamps simultaneously — 10-100x faster than row-by-row.

    Steps:
    1. Generate array of all minute timestamps for the month
    2. Extract hour and day arrays from timestamps
    3. Vectorized lookup of time-of-day factors per hour
    4. Build daily occupancy factor array
    5. Generate consumption for each usage type as numpy array
    6. Generate temperature arrays
    7. Assemble into pandas DataFrame

    Returns DataFrame with PARQUET_COLUMNS schema.
    """
    # ── timestamps ────────────────────────────────────────────────────────
    # start of month at 00:00 UTC, end at last minute of last day
    start_dt  = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
    days      = monthrange(year, month)[1]
    end_dt    = datetime(year, month, days, 23, 59, 0, tzinfo=timezone.utc)

    start_ts  = int(start_dt.timestamp())
    end_ts    = int(end_dt.timestamp())

    # all minute timestamps as numpy array
    timestamps = np.arange(start_ts, end_ts + 60, 60, dtype=np.int64)
    n          = len(timestamps)

    # extract hour and day for each timestamp — used for factor lookups
    hours      = (timestamps % 86400) // 3600                          # 0-23
    days_array = ((timestamps - start_ts) // 86400).astype(int) + 1   # 1-31

    # ── time of day factors ───────────────────────────────────────────────
    tod_factors = np.array(TIME_OF_DAY_FACTORS)[hours]                 # shape (n,)

    # ── occupancy factors per minute ──────────────────────────────────────
    # map day → factor for every minute
    occ_array   = np.array([daily_factors.get(d, 0.7) for d in days_array])

    # ── seasonal factors ──────────────────────────────────────────────────
    climate_zone   = CLIMATE_ZONE_BY_COUNTRY.get(site["country"], "northern_temperate")
    seasonal       = SEASONAL_FACTORS[climate_zone]
    elec_seasonal  = seasonal["electricity"][month - 1]
    gas_seasonal   = seasonal["natural_gas"][month - 1]

    # ── consumption generation ────────────────────────────────────────────
    consumption = {}
    total_weight = sum(USAGE_WEIGHTS.values())

    for usage_type, ranges in CONSUMPTION_RANGES.items():
        weight = USAGE_WEIGHTS[usage_type] / total_weight

        # seasonal factor — heating uses gas, others use electricity
        sf = gas_seasonal if usage_type == "heating" else elec_seasonal

        # time of day factor — special handling per usage type
        if usage_type in ("ups", "it"):
            # servers never fully off — floor at 0.6
            tf = np.maximum(tod_factors, 0.6)

        elif usage_type == "restaurant":
            # active only during meal hours — vectorized hour check
            meal_hours  = np.isin(hours, [7, 8, 12, 13, 17, 18, 19])
            tf          = np.where(meal_hours, 1.0, 0.05)

        elif usage_type == "lighting":
            # lighting follows occupancy closely
            tf = tod_factors * occ_array

        else:
            tf = tod_factors

        # gaussian noise for each minute — ±3% sensor variance
        noise = np.random.normal(loc=1.0, scale=0.03, size=n)

        if target_per_minute is not None:
            # target mode — scale to invoice alignment
            base   = target_per_minute * weight
            values = base * tf * sf * noise
        else:
            # free range mode — independent generation
            base   = np.random.uniform(ranges["min"], ranges["max"], size=n)
            values = base * tf * sf * occ_array * noise

        # floor at 0 — no negative consumption
        consumption[usage_type] = np.maximum(0.0, values).round(4)

    # ── temperature generation ────────────────────────────────────────────
    temp_range     = TEMPERATURE_RANGES[climate_zone]
    reference_temp = temp_range["reference_temp"]

    # seasonal ratio for temperature interpolation
    seasonal_ratio = (elec_seasonal - 0.2) / (1.7 - 0.2)
    seasonal_ratio = max(0.0, min(1.0, seasonal_ratio))
    base_temp      = temp_range["min"] + seasonal_ratio * (temp_range["max"] - temp_range["min"])

    # time of day sine variation — peaks at 3pm, trough at 4am
    time_variation  = 5.0 * np.sin(np.pi * (hours - 4) / 12)
    noise_temp      = np.random.normal(loc=0, scale=0.5, size=n)
    temperatures    = (base_temp + time_variation + noise_temp).round(2)

    # degree days
    ddc = np.maximum(0.0, temperatures - reference_temp).round(2)
    ddh = np.maximum(0.0, reference_temp - temperatures).round(2)

    # ── assemble DataFrame ────────────────────────────────────────────────
    df = pd.DataFrame({
        "site_id":            site["site_id"],
        "timestamp":          timestamps,
        "heating":            consumption["heating"],
        "cooling":            consumption["cooling"],
        "lighting":           consumption["lighting"],
        "ventilation":        consumption["ventilation"],
        "ups":                consumption["ups"],
        "it":                 consumption["it"],
        "restaurant":         consumption["restaurant"],
        "avg_outside_temp":   temperatures,
        "degree_day_cooling": ddc,
        "degree_day_heating": ddh,
        "reference_temp":     float(reference_temp),
    })

    return df


# ── Month Backfill ─────────────────────────────────────────────────────────

def backfill_month(site, year, month, cursor):
    """
    Generates and writes one monthly partition for one site.

    Steps:
    1. Check if partition already exists → skip if so
    2. Get invoice target → calculate target_per_minute + anomaly scenario
    3. Get daily occupancy factors from DB
    4. Generate all minute rows vectorized
    5. Write partition to Parquet
    6. Log row count, scenario, and timing

    Handles edge cases:
    - SITE_009 colocated after 2025-04-01 → skipped entirely
    - Missing invoice → falls back to free-range generation
    - SITE_010 inactive period → near-zero occupancy from DB handles it
    """
    # skip if partition already exists
    if not is_backfill_needed(site["site_id"], year, month):
        return

    # skip colocated site after effective date
    lifecycle = site.get("lifecycle_event", {})
    if lifecycle.get("event_type") == "colocated":
        colocated_date = datetime.strptime(
            lifecycle["effective_date"], "%Y-%m-%d"
        ).date()
        if date(year, month, 1) >= colocated_date:
            logger.info(f"Skipping {site['site_id']} {year}-{month:02d} — colocated")
            return

    start_time = time.time()

    # get invoice target for alignment
    target_per_minute, scenario = get_target_per_minute(
        site["site_id"], year, month
    )

    # get daily occupancy factors from DB
    daily_factors = get_daily_occupancy_factors(
        site["site_id"], year, month, cursor
    )

    # generate all minute rows vectorized
    df = generate_month_vectorized(
        site, year, month, daily_factors, target_per_minute
    )

    # write to parquet partition
    write_partition(df, site["site_id"], year, month)

    elapsed = round(time.time() - start_time, 2)
    logger.info(
        f"{site['site_id']} {year}-{month:02d} → "
        f"{len(df)} rows | scenario: {scenario or 'free-range'} | "
        f"{elapsed}s"
    )


# ── Site Backfill ──────────────────────────────────────────────────────────

def backfill_site(site, cursor):
    """
    Runs backfill for all months in the 2-year window for one site.

    Iterates month by month from start_date to today using relativedelta
    for clean month increments (handles Dec → Jan rollover).

    Logs total time for the site on completion.
    """
    start_time = time.time()

    start_date   = date.today() - relativedelta(years=BACKFILL_YEARS)
    end_date     = date.today()
    current_date = start_date.replace(day=1)  # always start from 1st of month

    logger.info(f"Starting backfill for {site['site_id']} — {site['site_name']}")

    while current_date <= end_date:
        backfill_month(site, current_date.year, current_date.month, cursor)
        current_date += relativedelta(months=1)

    elapsed = round(time.time() - start_time, 2)
    logger.info(f"{site['site_id']} backfill complete — {elapsed}s total")


# ── Run Backfill ───────────────────────────────────────────────────────────

def run_backfill(sites, cursor):
    """
    Entry point — runs full 2-year backfill for all 10 sites.

    Processes sites sequentially — one site fully complete before next.
    Logs total duration at the end.

    Idempotent — existing partitions are skipped automatically.
    Safe to re-run after partial failures or Docker restarts.
    """
    start_time = time.time()
    logger.info(f"Starting full backfill for {len(sites)} sites...")

    for site in sites:
        backfill_site(site, cursor)

    elapsed = round(time.time() - start_time, 2)
    logger.info(f"Full backfill complete — {elapsed}s total")