import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.shared_config import CLIMATE_ZONE_BY_COUNTRY, SEASONAL_FACTORS

# ── Consumption ranges per usage type (kWh per minute) ────────────────────
CONSUMPTION_RANGES = {
    "heating":     {"min": 0.5,  "max": 3.0},
    "cooling":     {"min": 0.5,  "max": 3.0},
    "lighting":    {"min": 0.1,  "max": 0.8},
    "ventilation": {"min": 0.1,  "max": 0.5},
    "ups":         {"min": 0.05, "max": 0.2},
    "it":          {"min": 0.3,  "max": 1.5},
    "restaurant":  {"min": 0.1,  "max": 0.8},
}

# ── Temperature ranges by climate zone ────────────────────────────────────
TEMPERATURE_RANGES = {
    "northern_temperate": {"min": -10, "max": 35,  "reference_temp": 18},
    "hot_arid":           {"min": 15,  "max": 48,  "reference_temp": 18},
    "tropical":           {"min": 22,  "max": 35,  "reference_temp": 18},
    "southern_temperate": {"min": -5,  "max": 38,  "reference_temp": 18},
}

# ── Time of day factors (hour 0-23) ───────────────────────────────────────
# Scales consumption based on time of day
# Peak hours 8am-6pm, low at night
TIME_OF_DAY_FACTORS = [
    0.2, 0.2, 0.2, 0.2,   # 00-03 night — minimal activity
    0.2, 0.3, 0.5, 0.7,   # 04-07 early morning — gradual ramp up
    0.9, 1.0, 1.0, 1.0,   # 08-11 morning peak
    1.0, 1.0, 1.0, 1.0,   # 12-15 afternoon peak
    0.9, 0.8, 0.6, 0.4,   # 16-19 evening wind down
    0.3, 0.2, 0.2, 0.2,   # 20-23 night
]

# ── Parquet storage settings ───────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.path.join(BASE_DIR, 'output', 'parquet')

# ── Backfill settings ──────────────────────────────────────────────────────
BACKFILL_YEARS = 2   # how many years of historical data to generate