# api_simulator/storage.py
#
# Data access layer for all Parquet read/write operations.
# This is the ONLY file in the system that touches Parquet files directly.
# All other modules (backfill, scheduler, api) go through this module.
#
# Partition structure:
#   output/parquet/SITE_001/year=2024/month=01/data.parquet
#
# Key behaviours:
#   - write_partition()      → writes full month partition (backfill)
#   - append_to_partition()  → hourly append with dedup (scheduler)
#   - read_partition()       → reads single month partition
#   - read_range()           → reads multiple partitions for API queries

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from api_config import PARQUET_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Column Schema ──────────────────────────────────────────────────────────
# Defines the exact column structure for all Parquet files.
# Must be consistent across all partitions to allow safe concat in read_range().

PARQUET_COLUMNS = [
    "site_id",
    "timestamp",            # unix int64
    "heating",
    "cooling",
    "lighting",
    "ventilation",
    "ups",
    "it",
    "restaurant",
    "avg_outside_temp",
    "degree_day_cooling",
    "degree_day_heating",
    "reference_temp",
]

# empty DataFrame with correct schema — returned when partition file not found
EMPTY_DF = pd.DataFrame(columns=PARQUET_COLUMNS)


# ── Partition Path ─────────────────────────────────────────────────────────

def get_partition_path(site_id, year, month):
    """
    Builds and returns the full file path for a monthly partition.

    Format: output/parquet/{site_id}/year={YYYY}/month={MM}/data.parquet
    Example: output/parquet/SITE_001/year=2024/month=01/data.parquet

    Month is zero-padded to 2 digits for consistent sorting.
    Does NOT create directories — that is write_partition()'s responsibility.
    """
    return os.path.join(
        PARQUET_DIR,
        site_id,
        f"year={year}",
        f"month={month:02d}",
        "data.parquet"
    )


# ── Write Partition ────────────────────────────────────────────────────────

def write_partition(df, site_id, year, month):
    """
    Writes a pandas DataFrame to the correct monthly partition path.

    Steps:
    1. Sort by timestamp — ensures chronological order in file
    2. Create all parent directories if they don't exist
    3. Write to Parquet using pyarrow engine with snappy compression
    4. Log row count and path

    Called by:
    - backfill.py  → writes full 44,640-row monthly partition once
    - append_to_partition() → overwrites current month after hourly append

    index=False — DataFrame index is not written to file.
    """
    partition_path = get_partition_path(site_id, year, month)

    # sort by timestamp before writing
    df = df.sort_values("timestamp").reset_index(drop=True)

    # create parent directories if they don't exist
    os.makedirs(os.path.dirname(partition_path), exist_ok=True)

    # write parquet with snappy compression — good balance of speed and size
    df.to_parquet(partition_path, index=False, engine="pyarrow", compression="snappy")

    logger.info(f"Written {len(df)} rows → {partition_path}")


# ── Read Partition ─────────────────────────────────────────────────────────

def read_partition(site_id, year, month):
    """
    Reads and returns a single monthly Parquet partition as a DataFrame.

    Returns EMPTY_DF (empty DataFrame with correct schema) if:
    - Partition file does not exist (backfill not yet run, or future month)
    - This ensures append_to_partition() can safely concat without None checks

    Uses pyarrow engine for consistent read/write behaviour.
    """
    partition_path = get_partition_path(site_id, year, month)

    if not os.path.exists(partition_path):
        logger.debug(f"Partition not found: {partition_path} — returning empty DataFrame")
        return EMPTY_DF.copy()

    df = pd.read_parquet(partition_path, engine="pyarrow")
    logger.debug(f"Read {len(df)} rows ← {partition_path}")
    return df


# ── Append to Partition ────────────────────────────────────────────────────

def append_to_partition(new_rows_df, site_id, year, month):
    """
    Appends new rows to an existing monthly partition.
    Called every hour by the scheduler for the current month.

    Steps:
    1. Read existing partition (returns empty DataFrame if not found)
    2. Concat existing rows + new rows
    3. Drop duplicate timestamps — makes operation idempotent
       (safe to re-run if scheduler restarts or Docker restarts mid-hour)
    4. Sort by timestamp
    5. Overwrite partition via write_partition()

    Deduplication is on 'timestamp' column —
    each minute has a unique unix timestamp so this is a safe key.

    Logs before/after row counts to detect any unexpected data loss.
    """
    existing_df = read_partition(site_id, year, month)
    rows_before = len(existing_df)

    # concat existing + new rows
    combined_df = pd.concat([existing_df, new_rows_df], ignore_index=True)

    # deduplicate on timestamp — keep last (new data takes priority)
    combined_df = combined_df.drop_duplicates(subset=["timestamp"], keep="last")

    # sort chronologically
    combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)

    rows_after = len(combined_df)
    new_rows   = rows_after - rows_before

    logger.info(
        f"{site_id} {year}-{month:02d} → "
        f"appended {new_rows} new rows "
        f"(total: {rows_after})"
    )

    write_partition(combined_df, site_id, year, month)


# ── Read Range ─────────────────────────────────────────────────────────────

def read_range(site_id, from_ts, to_ts):
    """
    Reads all Parquet partitions needed to cover a unix timestamp range.
    Called by API endpoints for consumption and temperature queries.

    Steps:
    1. Convert from_ts and to_ts to datetime objects
    2. Build list of all (year, month) combinations in the range
       Example: from Jan 15 to Mar 10 → [(2024,1), (2024,2), (2024,3)]
    3. Read each partition → list of DataFrames
    4. Concat all partitions into one DataFrame
    5. Filter rows to exact timestamp range (partitions may have extra rows)
    6. Return filtered DataFrame sorted by timestamp

    Returns empty DataFrame if no data found for the range.
    Handles cross-year ranges correctly (e.g. Dec 2024 → Jan 2025).
    """
    # convert unix timestamps to datetime objects
    from_dt = datetime.fromtimestamp(from_ts, tz=timezone.utc)
    to_dt   = datetime.fromtimestamp(to_ts,   tz=timezone.utc)

    # build list of (year, month) combinations to cover the full range
    partitions  = []
    current_dt  = from_dt.replace(day=1, hour=0, minute=0, second=0)

    while current_dt <= to_dt:
        partitions.append((current_dt.year, current_dt.month))

        # move to first day of next month
        if current_dt.month == 12:
            current_dt = current_dt.replace(year=current_dt.year + 1, month=1)
        else:
            current_dt = current_dt.replace(month=current_dt.month + 1)

    logger.debug(f"{site_id} range query covering {len(partitions)} partition(s): {partitions}")

    # read all relevant partitions
    dfs = [
        read_partition(site_id, year, month)
        for year, month in partitions
    ]

    # filter out empty DataFrames before concat
    dfs = [df for df in dfs if not df.empty]

    if not dfs:
        logger.warning(f"No data found for {site_id} between {from_ts} and {to_ts}")
        return EMPTY_DF.copy()

    # concat all partitions into one DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)

    # filter to exact timestamp range
    filtered_df = combined_df[
        (combined_df["timestamp"] >= from_ts) &
        (combined_df["timestamp"] <= to_ts)
    ].sort_values("timestamp").reset_index(drop=True)

    logger.debug(f"{site_id} → {len(filtered_df)} rows returned for range query")
    return filtered_df