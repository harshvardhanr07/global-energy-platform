# api_simulator/api.py
#
# FastAPI routes serving minute-level IoT sensor data from Parquet partitions.
# This is the external interface consumed by the Global Energy Platform.
#
# Routes:
#   GET /sites                          → list all 10 sites
#   GET /site/{site_id}/consumption     → electricity usage by type
#   GET /site/{site_id}/temperature     → temperature + degree day data
#
# All time-based queries use unix timestamps (integers).
# Data is read from Parquet partitions via storage.read_range().
#
# Validation:
#   - site_id must exist
#   - from_ts must be less than to_ts
#   - range cannot exceed 90 days

import os
import sys
import json
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from storage import read_range

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────

MAX_RANGE_SECONDS = 90 * 86400   # 90 days in seconds

# columns returned per endpoint — keeps responses clean and minimal
CONSUMPTION_COLUMNS  = [
    "timestamp", "heating", "cooling", "lighting",
    "ventilation", "ups", "it", "restaurant"
]
TEMPERATURE_COLUMNS  = [
    "timestamp", "avg_outside_temp",
    "degree_day_cooling", "degree_day_heating", "reference_temp"
]


# ── App Setup ──────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Global Energy IoT Simulator",
    description = "Serves minute-level electricity consumption and temperature data for 10 sites",
    version     = "1.0.0",
)


# ── Sites Data ─────────────────────────────────────────────────────────────

def load_sites():
    """
    Loads sites from config/sites.json at module level.
    Called once on startup — not on every request.
    Returns list of site dicts and a set of valid site IDs for O(1) lookup.
    """
    sites_path = os.path.join(BASE_DIR, 'config', 'sites.json')
    with open(sites_path) as f:
        sites = json.load(f)
    valid_ids = {site["site_id"] for site in sites}
    return sites, valid_ids


# load once at module level
SITES, VALID_SITE_IDS = load_sites()


# ── Validation ─────────────────────────────────────────────────────────────

def validate_request(site_id, from_ts, to_ts):
    """
    Validates site_id and timestamp range for all data endpoints.

    Raises HTTPException with appropriate status code and message if:
    - site_id not found in sites list              → 404
    - from_ts is greater than or equal to to_ts    → 400
    - range exceeds 90 days                        → 400

    Returns silently if all checks pass.
    """
    # site existence check — O(1) set lookup
    if site_id not in VALID_SITE_IDS:
        raise HTTPException(
            status_code = 404,
            detail      = f"Site '{site_id}' not found. Valid sites: {sorted(VALID_SITE_IDS)}"
        )

    # timestamp order check
    if from_ts >= to_ts:
        raise HTTPException(
            status_code = 400,
            detail      = f"from_ts ({from_ts}) must be less than to_ts ({to_ts})"
        )

    # range size check
    if (to_ts - from_ts) > MAX_RANGE_SECONDS:
        raise HTTPException(
            status_code = 400,
            detail      = f"Range cannot exceed 90 days. Requested: {round((to_ts - from_ts) / 86400, 1)} days"
        )


# ── Routes ─────────────────────────────────────────────────────────────────

@app.get(
    "/sites",
    summary     = "List all sites",
    description = "Returns metadata for all 10 sites including status, location, and timezone",
)
def get_sites():
    """
    Returns a clean list of all site metadata from sites.json.

    Excludes lifecycle_event field — internal generation detail
    not relevant to API consumers.
    """
    clean_sites = [
        {
            "site_id":      site["site_id"],
            "site_name":    site["site_name"],
            "country":      site["country"],
            "city":         site["city"],
            "timezone":     site["timezone"],
            "status":       site["status"],
            "active_date":  site["active_date"],
            "inactive_date":site["inactive_date"],
            "site_sqm":     site["site_sqm"],
            "site_capacity":site["site_capacity"],
            "billing_cycle":site["billing_cycle"],
            "energy_types": site["energy_types"],
        }
        for site in SITES
    ]

    logger.info(f"GET /sites → {len(clean_sites)} sites returned")
    return clean_sites


@app.get(
    "/site/{site_id}/consumption",
    summary     = "Get electricity consumption data",
    description = "Returns minute-level electricity consumption by usage type for a site over a unix timestamp range",
)
def get_consumption(
    site_id: str,
    from_ts: int = Query(..., description="Start of range as unix timestamp (seconds)"),
    to_ts:   int = Query(..., description="End of range as unix timestamp (seconds)"),
):
    """
    Returns minute-level electricity consumption broken down into
    7 usage types: heating, cooling, lighting, ventilation, ups, it, restaurant.

    Each row represents one minute of sensor data.
    Timestamps are unix integers (seconds since epoch UTC).

    Returns empty data list with rows=0 if no data found for the range —
    does not raise an error, allows caller to handle gracefully.
    """
    validate_request(site_id, from_ts, to_ts)

    df = read_range(site_id, from_ts, to_ts)

    if df.empty:
        logger.warning(f"GET /site/{site_id}/consumption — no data for range {from_ts}→{to_ts}")
        return {
            "site_id": site_id,
            "from_ts": from_ts,
            "to_ts":   to_ts,
            "rows":    0,
            "data":    [],
        }

    # select only consumption columns
    result_df = df[CONSUMPTION_COLUMNS].copy()

    # convert to int to ensure clean JSON serialization
    result_df["timestamp"] = result_df["timestamp"].astype(int)

    data = result_df.to_dict(orient="records")

    logger.info(
        f"GET /site/{site_id}/consumption "
        f"| range: {from_ts}→{to_ts} "
        f"| rows: {len(data)}"
    )

    return {
        "site_id": site_id,
        "from_ts": from_ts,
        "to_ts":   to_ts,
        "rows":    len(data),
        "data":    data,
    }


@app.get(
    "/site/{site_id}/temperature",
    summary     = "Get temperature and degree day data",
    description = "Returns minute-level temperature and degree day data for a site over a unix timestamp range",
)
def get_temperature(
    site_id: str,
    from_ts: int = Query(..., description="Start of range as unix timestamp (seconds)"),
    to_ts:   int = Query(..., description="End of range as unix timestamp (seconds)"),
):
    """
    Returns minute-level temperature data including:
    - avg_outside_temp   : ambient external temperature in °C
    - degree_day_cooling : how much warmer than reference (cooling needed)
    - degree_day_heating : how much cooler than reference (heating needed)
    - reference_temp     : base temperature for degree day calculation (18°C)

    Each row represents one minute of sensor data.
    Timestamps are unix integers (seconds since epoch UTC).

    Returns empty data list with rows=0 if no data found for the range.
    """
    validate_request(site_id, from_ts, to_ts)

    df = read_range(site_id, from_ts, to_ts)

    if df.empty:
        logger.warning(f"GET /site/{site_id}/temperature — no data for range {from_ts}→{to_ts}")
        return {
            "site_id": site_id,
            "from_ts": from_ts,
            "to_ts":   to_ts,
            "rows":    0,
            "data":    [],
        }

    # select only temperature columns
    result_df = df[TEMPERATURE_COLUMNS].copy()

    # convert timestamp to int for clean JSON serialization
    result_df["timestamp"] = result_df["timestamp"].astype(int)

    data = result_df.to_dict(orient="records")

    logger.info(
        f"GET /site/{site_id}/temperature "
        f"| range: {from_ts}→{to_ts} "
        f"| rows: {len(data)}"
    )

    return {
        "site_id": site_id,
        "from_ts": from_ts,
        "to_ts":   to_ts,
        "rows":    len(data),
        "data":    data,
    }


# ── Health Check ───────────────────────────────────────────────────────────

@app.get(
    "/health",
    summary     = "Health check",
    description = "Returns API status — used by Docker healthcheck and monitoring",
)
def health_check():
    """
    Simple health check endpoint.
    Returns 200 with status ok if API is running.
    Used by Docker healthcheck and load balancers.
    """
    return {"status": "ok", "sites_loaded": len(SITES)}