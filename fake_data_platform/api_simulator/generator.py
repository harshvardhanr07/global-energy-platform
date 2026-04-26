# api_simulator/generator.py
#
# Generates one minute of sensor data for one site.
# Each row simulates what a real IoT sensor would report —
# electricity consumption by usage type + environmental temperature data.
#
# Key design principle — invoice alignment:
#   Monthly API aggregate MUST align with invoice CSV consumption.
#   A target_per_minute is calculated from the invoice and passed in,
#   ensuring the sum of all minute rows ≈ invoice total for that month.
#
# Anomaly simulation:
#   90% of months → API slightly under invoice (normal sensor behaviour)
#    5% of months → API higher than invoice (flag — billing anomaly)
#    5% of months → API much lower than invoice (flag — sensor outage)
#
# Consumption scaling per minute:
#   base     = target_per_minute distributed across 7 usage types by weight
#   adjusted = base × time_of_day_factor × seasonal_factor × noise
#
# Temperature derived from:
#   climate zone range + seasonal factor + time of day sine variation

import os
import sys
import numpy as np
import pandas as pd
import logging
from datetime import datetime, timezone
from calendar import monthrange

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from api_config import (
    CONSUMPTION_RANGES,
    TEMPERATURE_RANGES,
    TIME_OF_DAY_FACTORS,
    CLIMATE_ZONE_BY_COUNTRY,
    SEASONAL_FACTORS,
    ANOMALY_CONFIG,
    ANOMALY_FACTORS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# output directory for invoice CSVs
CSV_DIR = os.path.join(BASE_DIR, 'output', 'csv')

# ── Usage type weights ─────────────────────────────────────────────────────
# Defines how total electricity is distributed across 7 usage types.
# Weights are relative — they are normalised to sum to 1.0 at runtime.
# Based on typical commercial building energy breakdown.
USAGE_WEIGHTS = {
    "heating":     0.25,
    "cooling":     0.20,
    "lighting":    0.15,
    "ventilation": 0.10,
    "ups":         0.05,
    "it":          0.15,
    "restaurant":  0.10,
}


# ── Invoice Reader ─────────────────────────────────────────────────────────

def get_invoice_target(site_id, year, month):
    """
    Reads the monthly invoice CSV and returns total electricity consumption
    for the given site and month.

    This value becomes the monthly target that API minute-level data
    must aggregate to — ensuring cross-source alignment.

    Returns None if:
    - Invoice CSV file doesn't exist (missing invoice simulation)
    - Site has no electricity row in that month's invoice

    Caller should fall back to free-range generation if None returned.
    """
    filename = f"invoices_{year}_{month:02d}.csv"
    filepath = os.path.join(CSV_DIR, filename)

    if not os.path.exists(filepath):
        logger.warning(f"Invoice file not found: {filename} — using free range generation")
        return None

    try:
        df = pd.read_csv(filepath)

        # filter for this site and electricity consumption type
        mask = (
            (df["site_id"] == site_id) &
            (df["consumption_type"] == "electricity")
        )
        row = df[mask]

        if row.empty:
            logger.warning(f"No electricity row for {site_id} in {filename}")
            return None

        target = float(row["consumption"].values[0])
        logger.debug(f"Invoice target for {site_id} {year}-{month:02d}: {target} kWh")
        return target

    except Exception as e:
        logger.error(f"Failed to read invoice target for {site_id} {year}-{month:02d}: {e}")
        return None


# ── Anomaly Factor ─────────────────────────────────────────────────────────

def get_anomaly_factor():
    """
    Randomly selects an anomaly scenario for a site-month combination.

    Scenarios:
    - normal (90%): API slightly under invoice — expected sensor behaviour
    - high   ( 5%): API higher than invoice — billing/overcounting anomaly
    - low    ( 5%): API much lower than invoice — sensor outage/data loss

    Returns (scenario_name, scaling_factor) tuple.
    The scaling factor is applied to the invoice target to get the
    actual API monthly total before distributing across minutes.
    """
    roll = np.random.random()

    if roll < ANOMALY_CONFIG["normal_probability"]:
        factor = np.random.uniform(
            ANOMALY_FACTORS["normal"]["min"],
            ANOMALY_FACTORS["normal"]["max"]
        )
        return "normal", factor

    elif roll < ANOMALY_CONFIG["normal_probability"] + ANOMALY_CONFIG["high_anomaly_probability"]:
        factor = np.random.uniform(
            ANOMALY_FACTORS["high"]["min"],
            ANOMALY_FACTORS["high"]["max"]
        )
        return "high", factor

    else:
        factor = np.random.uniform(
            ANOMALY_FACTORS["low"]["min"],
            ANOMALY_FACTORS["low"]["max"]
        )
        return "low", factor


# ── Temperature Generation ─────────────────────────────────────────────────

def get_temperature(site, timestamp):
    """
    Generates realistic temperature data for a site at a given unix timestamp.

    Steps:
    1. Resolve climate zone from site country
    2. Get seasonal multiplier for the month
    3. Interpolate temperature within climate zone range using seasonal factor
    4. Apply time-of-day variation via sine wave
       - Coolest at 4am, warmest at 3pm
       - ±5°C swing across the day
    5. Add small gaussian noise ±0.5°C
    6. Calculate degree days against reference temperature (18°C)

    Degree Day Cooling (DDC): max(0, temp - reference) — cooling needed
    Degree Day Heating (DDH): max(0, reference - temp) — heating needed

    Returns dict with 4 temperature fields.
    """
    dt           = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    month        = dt.month
    hour         = dt.hour

    climate_zone   = CLIMATE_ZONE_BY_COUNTRY.get(site["country"], "northern_temperate")
    temp_range     = TEMPERATURE_RANGES[climate_zone]
    reference_temp = temp_range["reference_temp"]

    # use electricity seasonal factor as proxy for overall climate activity
    seasonal_factor = SEASONAL_FACTORS[climate_zone]["electricity"][month - 1]

    # normalise seasonal factor to 0-1 range for temperature interpolation
    seasonal_ratio = (seasonal_factor - 0.2) / (1.7 - 0.2)
    seasonal_ratio = max(0.0, min(1.0, seasonal_ratio))  # cap to 0-1

    # interpolate base temperature within climate zone range
    base_temp = temp_range["min"] + seasonal_ratio * (temp_range["max"] - temp_range["min"])

    # time of day sine variation — peaks at 3pm (hour=15), trough at 4am (hour=4)
    time_variation = 5.0 * np.sin(np.pi * (hour - 4) / 12)

    # final temperature with gaussian noise
    temp = base_temp + time_variation + np.random.normal(loc=0, scale=0.5)
    temp = round(temp, 2)

    return {
        "avg_outside_temp":   temp,
        "degree_day_cooling": round(max(0.0, temp - reference_temp), 2),
        "degree_day_heating": round(max(0.0, reference_temp - temp), 2),
        "reference_temp":     float(reference_temp),
    }


# ── Consumption Generation ─────────────────────────────────────────────────

def get_consumption(site, timestamp, occupancy_factor, target_per_minute=None):
    """
    Generates electricity consumption for all 7 usage types for one minute.

    Two modes:

    Target mode (target_per_minute provided):
        Total consumption for the minute is driven by target_per_minute.
        Usage types are split by USAGE_WEIGHTS then scaled by their
        individual time-of-day and seasonal adjustments.
        This ensures monthly aggregate aligns with invoice.

    Free range mode (target_per_minute is None):
        Used when no invoice exists for that month.
        Each usage type generated independently from CONSUMPTION_RANGES.

    Special handling per usage type:
    - IT / UPS:     time factor floored at 0.6 — servers never fully off
    - Restaurant:   near-zero outside meal hours (7-9, 12-14, 17-20)
    - Lighting:     scaled directly by occupancy
    - Heating:      uses natural_gas seasonal factor
    - Cooling:      uses electricity seasonal factor

    Returns dict of 7 usage type values in kWh.
    """
    dt    = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    month = dt.month
    hour  = dt.hour

    climate_zone = CLIMATE_ZONE_BY_COUNTRY.get(site["country"], "northern_temperate")
    time_factor  = TIME_OF_DAY_FACTORS[hour]
    seasonal     = SEASONAL_FACTORS[climate_zone]

    # per-usage-type time and seasonal multipliers
    def effective_factor(usage_type):
        if usage_type == "heating":
            sf = seasonal["natural_gas"][month - 1]
        else:
            sf = seasonal["electricity"][month - 1]

        if usage_type in ("ups", "it"):
            tf = max(0.6, time_factor)
        elif usage_type == "restaurant":
            tf = 1.0 if hour in (7, 8, 12, 13, 17, 18, 19) else 0.05
        elif usage_type == "lighting":
            tf = time_factor * occupancy_factor
        else:
            tf = time_factor

        return tf * sf

    rows = {}

    if target_per_minute is not None:
        # ── target mode — distribute target across usage types by weight ──
        total_weight = sum(USAGE_WEIGHTS.values())

        for usage_type in CONSUMPTION_RANGES:
            weight  = USAGE_WEIGHTS[usage_type] / total_weight
            base    = target_per_minute * weight
            factor  = effective_factor(usage_type)
            noise   = np.random.normal(loc=1.0, scale=0.03)
            value   = base * factor * noise
            rows[usage_type] = round(max(0.0, value), 4)

    else:
        # ── free range mode — independent generation per usage type ───────
        for usage_type, ranges in CONSUMPTION_RANGES.items():
            base   = np.random.uniform(ranges["min"], ranges["max"])
            factor = effective_factor(usage_type)
            noise  = np.random.normal(loc=1.0, scale=0.03)
            value  = base * factor * occupancy_factor * noise
            rows[usage_type] = round(max(0.0, value), 4)

    return rows


# ── Minute Row Generator ───────────────────────────────────────────────────

def generate_minute(site, timestamp, occupancy_factor, target_per_minute=None):
    """
    Generates one complete sensor reading for one site at one unix timestamp.

    Combines temperature and consumption into a single flat dict
    ready to be appended to a pandas DataFrame row.

    occupancy_factor and target_per_minute are pre-calculated by the
    caller (backfill or scheduler) once per day/month respectively —
    avoiding repeated DB queries or CSV reads inside the minute loop.

    Returns a single dict with 13 fields.
    """
    temperature = get_temperature(site, timestamp)
    consumption = get_consumption(site, timestamp, occupancy_factor, target_per_minute)

    return {
        "site_id":            site["site_id"],
        "timestamp":          int(timestamp),
        "heating":            consumption["heating"],
        "cooling":            consumption["cooling"],
        "lighting":           consumption["lighting"],
        "ventilation":        consumption["ventilation"],
        "ups":                consumption["ups"],
        "it":                 consumption["it"],
        "restaurant":         consumption["restaurant"],
        "avg_outside_temp":   temperature["avg_outside_temp"],
        "degree_day_cooling": temperature["degree_day_cooling"],
        "degree_day_heating": temperature["degree_day_heating"],
        "reference_temp":     temperature["reference_temp"],
    }


# ── Monthly Target Calculator ──────────────────────────────────────────────

def get_target_per_minute(site_id, year, month):
    """
    Calculates the per-minute electricity target for a site-month.

    Steps:
    1. Read invoice CSV → get monthly electricity total
    2. Apply anomaly factor → scale target up or down
    3. Divide by total minutes in the month
    4. Log if anomaly scenario selected

    Returns (target_per_minute, scenario) tuple.
    Returns (None, None) if no invoice found.
    """
    invoice_total = get_invoice_target(site_id, year, month)

    if invoice_total is None:
        return None, None

    scenario, anomaly_factor = get_anomaly_factor()

    # log anomalies so they can be detected by downstream pipeline
    if scenario != "normal":
        logger.warning(
            f"ANOMALY [{scenario.upper()}] {site_id} {year}-{month:02d} "
            f"— invoice: {invoice_total} kWh, factor: {anomaly_factor:.3f}"
        )

    scaled_total   = invoice_total * anomaly_factor
    total_minutes  = monthrange(year, month)[1] * 24 * 60
    target_per_min = scaled_total / total_minutes

    return target_per_min, scenario