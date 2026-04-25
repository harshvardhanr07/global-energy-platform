# csv_generator/generator.py
#
# Core generation logic for monthly invoice data.
# Responsible for:
#   - Determining billing periods per site
#   - Checking site activity for a given period
#   - Fetching occupancy factors from the DB
#   - Applying seasonal multipliers by climate zone
#   - Calculating consumption and cost per energy type
#   - Assembling final row dicts for CSV export

import numpy as np
import logging
from datetime import date
from calendar import monthrange

from config import (
    UNITS_BY_COUNTRY,
    BASE_CONSUMPTION_PER_SQM,
    UNIT_CONVERSION,
    COST_PER_UNIT,
    SEASONAL_FACTORS,
    CLIMATE_ZONE_BY_COUNTRY,
    CURRENCY_BY_COUNTRY,
    MISSING_MONTH_PROBABILITY,
    COST_ROUNDING_VARIATION,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Billing Period ─────────────────────────────────────────────────────────

def get_billing_period(site, year, month):
    """
    Returns (from_date, to_date) tuple based on the site's billing cycle.

    calendar  → 1st to last day of the calendar month
    mid_month → 15th of current month to 14th of next month

    Mid-month billing creates cross-month periods that require special
    handling in the downstream pipeline when aggregating by calendar month.
    """
    if site["billing_cycle"] == "calendar":
        from_date = date(year, month, 1)
        to_date   = date(year, month, monthrange(year, month)[1])
    else:
        # mid_month billing — handle December rollover to January
        from_date = date(year, month, 15)
        if month == 12:
            to_date = date(year + 1, 1, 14)
        else:
            to_date = date(year, month + 1, 14)

    return from_date, to_date


# ── Site Activity Check ────────────────────────────────────────────────────

def is_site_active(site, from_date, to_date):
    """
    Returns True if the site was operational during any part of the billing period.

    Handles two inactive scenarios:
    - Site not yet opened (billing period is before active_date)
    - Site permanently closed (billing period starts after inactive_date)

    Note: Temporary inactive periods (e.g. SITE_010 renovation) are NOT
    handled here — those affect occupancy factor, not site activity.
    """
    active_date = date.fromisoformat(site["active_date"])

    # ✅ fixed: use string key with quotes
    inactive_date = (
        date.fromisoformat(site["inactive_date"])
        if site["inactive_date"] else None
    )

    # billing period ends before site opened
    if to_date < active_date:
        return False

    # billing period starts after site was permanently closed
    if inactive_date and from_date >= inactive_date:
        return False

    return True


# ── Occupancy Factor ───────────────────────────────────────────────────────

def get_occupancy_factor(site_id, year, month, cursor) -> float:
    """
    Queries site_occupancy table for average occupancy and capacity in the given month.
    Returns occupancy_factor = avg_occupancy / avg_capacity, capped between 0.1 and 1.0.

    Falls back to 0.7 (reasonable office utilisation) if:
    - No occupancy data exists for that month (missing data simulation)
    - Query returns nulls (site had no recorded attendance)

    Rounding to 1 decimal keeps the factor clean for multiplication.
    """
    cursor.execute("""
        SELECT
            AVG(site_capacity),
            AVG(occupancy)
        FROM site_occupancy
        WHERE site_id = %s
          AND EXTRACT(YEAR  FROM date) = %s
          AND EXTRACT(MONTH FROM date) = %s
    """, (site_id, year, month))

    result = cursor.fetchone()

    # ✅ fixed: guard against None before division
    if result is None or result[0] is None or result[1] is None:
        logger.warning(f"No occupancy data for {site_id} {year}-{month:02d} — using default 0.7")
        return 0.7

    occupancy_factor = float(result[1]) / float(result[0])

    # cap between 0.1 (near-empty) and 1.0 (full capacity)
    return round(max(0.1, min(1.0, occupancy_factor)), 1)


# ── Seasonal Factor ────────────────────────────────────────────────────────

def get_seasonal_factor(site, month) -> dict:
    """
    Returns a dict of seasonal multipliers per energy type for the given month.

    Looks up the site's country → climate zone → monthly multiplier array.
    month is 1-indexed, array is 0-indexed hence month-1.

    Example return for London in January (northern_temperate):
    {
        "electricity": 1.1,
        "natural_gas": 1.6,
        "district_energy": 1.5,
        "diesel": 1.2
    }
    """
    climate_zone = CLIMATE_ZONE_BY_COUNTRY.get(site["country"], "northern_temperate")
    factors      = SEASONAL_FACTORS[climate_zone]

    return {
        energy_type: factors[energy_type][month - 1]
        for energy_type in factors
    }


# ── Consumption Calculation ────────────────────────────────────────────────

def calculate_consumption(site, energy_type, month, occupancy_factor, seasonal_factor):
    """
    Calculates consumption for one energy type for one billing month.

    Formula:
        base     = site_sqm × random_rate (from BASE_CONSUMPTION_PER_SQM range)
        adjusted = base × occupancy_factor × seasonal_factor × noise (±5%)

    Then converts from base unit to the country-specific unit using UNIT_CONVERSION.

    Returns (consumption, unit) or (None, None) if energy type
    is not available in the site's country (e.g. no natural gas in UAE).
    """
    base_range = BASE_CONSUMPTION_PER_SQM[energy_type]
    base_rate  =  np.random.uniform(base_range["min"], base_range["max"])
   
    base       = site["site_sqm"] * base_rate

    seasonal   = seasonal_factor.get(energy_type, 1.0)
    noise = np.random.normal(loc=1.0, scale=0.03)

    adjusted   = base * occupancy_factor * seasonal * noise

    # get the correct unit for this country
    country_units = UNITS_BY_COUNTRY.get(site["country"], {})
    target_unit   = country_units.get(energy_type)

    if target_unit is None:
        # energy type not supported in this country — skip silently
        return None, None

    # convert from base unit to target unit
    conversion_factor = UNIT_CONVERSION.get(target_unit, 1.0)
    consumption       = round(adjusted * conversion_factor, 2)

    return consumption, target_unit


# ── Cost Calculation ───────────────────────────────────────────────────────

def calculate_cost(consumption, site, energy_type):
    """
    Calculates total cost and unit rate for the given consumption.

    Unit rate is randomly selected within the country's realistic range
    then nudged slightly by COST_ROUNDING_VARIATION to simulate
    month-to-month tariff fluctuations and invoice rounding.

    Returns (total_cost, unit_rate, currency) or (None, None, None)
    if no rate data exists for this country/energy type combination.
    """
    rate_range = COST_PER_UNIT.get(site["country"], {}).get(energy_type)

    if rate_range is None:
        return None, None, None

    # apply tariff variance
    unit_rate  = np.random.uniform(rate_range["min"], rate_range["max"])
    unit_rate += np.random.normal(loc=0, scale=COST_ROUNDING_VARIATION)
    unit_rate  = round(unit_rate, 4)

    total_cost = round(consumption * unit_rate, 2)
    currency   = CURRENCY_BY_COUNTRY.get(site["country"], "USD")

    return total_cost, unit_rate, currency


# ── Month Generator ────────────────────────────────────────────────────────

def generate_month(site, year, month, cursor):
    """
    Generates all invoice rows for one site for one billing month.
    Returns a list of row dicts — one dict per energy type.

    Returns empty list (no rows) if:
    - Site was not active during the billing period
    - Random missing invoice simulation triggers (~2% chance)

    Each row dict maps directly to CSV columns defined in the data spec.
    """
    from_date, to_date = get_billing_period(site, year, month)

    # skip inactive sites for this period
    if not is_site_active(site, from_date, to_date):
        logger.debug(f"{site['site_id']} not active for {year}-{month:02d} — skipping")
        return []

    # simulate missing invoice (~2% chance per site per month)
    if np.random.random() < MISSING_MONTH_PROBABILITY:
        logger.warning(f"{site['site_id']} invoice missing for {year}-{month:02d} — simulated gap")
        return []

    occupancy_factor = get_occupancy_factor(site["site_id"], year, month, cursor)
    seasonal_factors = get_seasonal_factor(site, month)

    rows = []

    # ✅ fixed: energy_types (with 's')
    for energy_type in site["energy_types"]:
        consumption, unit = calculate_consumption(
            site, energy_type, month, occupancy_factor, seasonal_factors
        )

        if consumption is None:
            continue  # energy type not available in this country

        total_cost, unit_rate, currency = calculate_cost(consumption, site, energy_type)

        if total_cost is None:
            continue  # no cost data for this country/energy type

        rows.append({
            "site_id":                   site["site_id"],
            "site_name":                 site["site_name"],
            "billing_period_from":       from_date.isoformat(),
            "billing_period_to":         to_date.isoformat(),
            "consumption_type":          energy_type,
            "consumption":               consumption,
            "consumption_unit":          unit,
            "consumption_cost":          total_cost,
            "consumption_cost_unit":     currency,
            "cost_per_consumption_unit": unit_rate,
        })

    logger.debug(f"{site['site_id']} {year}-{month:02d} → {len(rows)} rows generated")
    return rows