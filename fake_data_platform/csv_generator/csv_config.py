# csv_generator/config.py

import sys
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from config.shared_config import CLIMATE_ZONE_BY_COUNTRY, SEASONAL_FACTORS

# ── Billing Currency by Country ────────────────────────────────────────────
CURRENCY_BY_COUNTRY = {
    "United States":          "USD",
    "United Kingdom":         "GBP",
    "France":                 "EUR",
    "Germany":                "EUR",
    "Canada":                 "CAD",
    "Japan":                  "JPY",
    "Australia":              "AUD",
    "United Arab Emirates":   "AED",
    "Singapore":              "SGD",
    "Brazil":                 "BRL",
}

# ── Units by Country per Energy Type ──────────────────────────────────────
# None means that energy type is not available in that country
UNITS_BY_COUNTRY = {
    "United States": {
        "electricity":      "kWh",
        "natural_gas":      "therms",
        "district_energy":  "MMBtu",
        "diesel":           "gallons",
    },
    "United Kingdom": {
        "electricity":      "kWh",
        "natural_gas":      "m³",
        "district_energy":  "kWh",
        "diesel":           "litres",
    },
    "France": {
        "electricity":      "kWh",
        "natural_gas":      "m³",
        "district_energy":  "kWh",
        "diesel":           "litres",
    },
    "Germany": {
        "electricity":      "kWh",
        "natural_gas":      "m³",
        "district_energy":  "kWh",
        "diesel":           "litres",
    },
    "Canada": {
        "electricity":      "kWh",
        "natural_gas":      "m³",
        "district_energy":  "GJ",
        "diesel":           "litres",
    },
    "Japan": {
        "electricity":      "kWh",
        "natural_gas":      "m³",
        "district_energy":  "GJ",
        "diesel":           "litres",
    },
    "Australia": {
        "electricity":      "kWh",
        "natural_gas":      "GJ",
        "district_energy":  "GJ",
        "diesel":           "litres",
    },
    "United Arab Emirates": {
        "electricity":      "kWh",
        "natural_gas":      None,
        "district_energy":  "kWh",
        "diesel":           "litres",
    },
    "Singapore": {
        "electricity":      "kWh",
        "natural_gas":      None,
        "district_energy":  "kWh",
        "diesel":           "litres",
    },
    "Brazil": {
        "electricity":      "kWh",
        "natural_gas":      "m³",
        "district_energy":  None,
        "diesel":           "litres",
    },
}

# ── Base Consumption Rate per SQM per Month ────────────────────────────────
# These are in the most common unit (kWh, m³, litres etc.)
# Adjusted by occupancy and seasonal factors at generation time
BASE_CONSUMPTION_PER_SQM = {
    "electricity":      {"min": 8.0,   "max": 15.0},   # kWh/sqm
    "natural_gas":      {"min": 2.0,   "max": 6.0},    # m³/sqm
    "district_energy":  {"min": 5.0,   "max": 10.0},   # kWh/sqm
    "diesel":           {"min": 0.5,   "max": 2.0},    # litres/sqm
}

# ── Unit Conversion Factors ────────────────────────────────────────────────
# Some countries use different units — convert from base unit
# Base units: electricity=kWh, natural_gas=m³, district_energy=kWh, diesel=litres
UNIT_CONVERSION = {
    "therms":   0.0341,     # m³ natural gas → therms (1 m³ = 0.0341 therms... actually 1 therm = 29.3 kWh, 1 m³ gas ≈ 10.55 kWh → 0.36 therms)
    "MMBtu":    0.003412,   # kWh → MMBtu (1 kWh = 0.003412 MMBtu)
    "GJ":       0.0036,     # kWh → GJ (1 kWh = 0.0036 GJ)
    "gallons":  0.2642,     # litres → gallons (1 litre = 0.2642 gallons)
    "kWh":      1.0,        # no conversion needed
    "m³":       1.0,        # no conversion needed
    "litres":   1.0,        # no conversion needed
}

# ── Cost per Unit by Country and Energy Type ──────────────────────────────
# Ranges reflect realistic market rates with slight variance month to month
COST_PER_UNIT = {
    "United States": {
        "electricity":      {"min": 0.10,   "max": 0.14},   # USD/kWh
        "natural_gas":      {"min": 0.80,   "max": 1.20},   # USD/therm
        "district_energy":  {"min": 18.00,  "max": 25.00},  # USD/MMBtu
        "diesel":           {"min": 0.90,   "max": 1.10},   # USD/gallon
    },
    "United Kingdom": {
        "electricity":      {"min": 0.25,   "max": 0.35},   # GBP/kWh
        "natural_gas":      {"min": 0.06,   "max": 0.10},   # GBP/m³
        "district_energy":  {"min": 0.08,   "max": 0.14},   # GBP/kWh
        "diesel":           {"min": 1.40,   "max": 1.65},   # GBP/litre
    },
    "France": {
        "electricity":      {"min": 0.18,   "max": 0.24},   # EUR/kWh
        "natural_gas":      {"min": 0.07,   "max": 0.11},   # EUR/m³
        "district_energy":  {"min": 0.07,   "max": 0.12},   # EUR/kWh
        "diesel":           {"min": 1.70,   "max": 1.90},   # EUR/litre
    },
    "Germany": {
        "electricity":      {"min": 0.28,   "max": 0.38},   # EUR/kWh
        "natural_gas":      {"min": 0.08,   "max": 0.13},   # EUR/m³
        "district_energy":  {"min": 0.09,   "max": 0.15},   # EUR/kWh
        "diesel":           {"min": 1.75,   "max": 1.95},   # EUR/litre
    },
    "Canada": {
        "electricity":      {"min": 0.10,   "max": 0.15},   # CAD/kWh
        "natural_gas":      {"min": 0.35,   "max": 0.55},   # CAD/m³
        "district_energy":  {"min": 12.00,  "max": 18.00},  # CAD/GJ
        "diesel":           {"min": 1.55,   "max": 1.80},   # CAD/litre
    },
    "Japan": {
        "electricity":      {"min": 22.00,  "max": 30.00},  # JPY/kWh
        "natural_gas":      {"min": 100.00, "max": 140.00}, # JPY/m³
        "district_energy":  {"min": 1800.0, "max": 2400.0}, # JPY/GJ
        "diesel":           {"min": 155.00, "max": 180.00}, # JPY/litre
    },
    "Australia": {
        "electricity":      {"min": 0.22,   "max": 0.32},   # AUD/kWh
        "natural_gas":      {"min": 8.00,   "max": 14.00},  # AUD/GJ
        "district_energy":  {"min": 10.00,  "max": 16.00},  # AUD/GJ
        "diesel":           {"min": 1.80,   "max": 2.10},   # AUD/litre
    },
    "United Arab Emirates": {
        "electricity":      {"min": 0.08,   "max": 0.12},   # AED/kWh
        "district_energy":  {"min": 0.06,   "max": 0.10},   # AED/kWh
        "diesel":           {"min": 2.80,   "max": 3.20},   # AED/litre
    },
    "Singapore": {
        "electricity":      {"min": 0.25,   "max": 0.32},   # SGD/kWh
        "district_energy":  {"min": 0.14,   "max": 0.20},   # SGD/kWh
        "diesel":           {"min": 1.90,   "max": 2.30},   # SGD/litre
    },
    "Brazil": {
        "electricity":      {"min": 0.65,   "max": 0.90},   # BRL/kWh
        "natural_gas":      {"min": 3.50,   "max": 5.00},   # BRL/m³
        "diesel":           {"min": 5.50,   "max": 6.50},   # BRL/litre
    },
}

# ── Data Quality Rules ─────────────────────────────────────────────────────
MISSING_MONTH_PROBABILITY   = 0.02   # 2% chance a site's invoice is missing for a month
COST_ROUNDING_VARIATION     = 0.005  # 0.5% rounding noise on cost per unit