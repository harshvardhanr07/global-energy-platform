# ── Seasonal Factors by Climate Zone ──────────────────────────────────────
# Multiplier applied to consumption per month (1–12)
# Reflects heating/cooling demand by season and hemisphere

SEASONAL_FACTORS = {

    # Northern hemisphere — cold winters, warm summers
    # Heavy heating in winter → high gas/district energy
    # Moderate cooling in summer → slightly higher electricity
    "northern_temperate": {
        "electricity":     [1.1, 1.0, 0.9, 0.85, 0.9, 1.0, 1.15, 1.15, 1.0, 0.9, 1.0, 1.1],
        "natural_gas":     [1.6, 1.5, 1.2, 0.8,  0.5, 0.3, 0.2,  0.2,  0.5, 0.9, 1.3, 1.6],
        "district_energy": [1.5, 1.4, 1.1, 0.8,  0.6, 0.4, 0.3,  0.3,  0.6, 0.9, 1.2, 1.5],
        "diesel":          [1.2, 1.1, 1.0, 1.0,  1.0, 1.0, 1.0,  1.0,  1.0, 1.0, 1.1, 1.2],
    },

    # Desert / tropical — hot all year, no winter
    # Cooling dominates → high electricity year round, peak in summer
    "hot_arid": {
        
        "electricity":     [1.1, 1.1, 1.2, 1.3, 1.5, 1.6, 1.7, 1.7, 1.5, 1.3, 1.1, 1.0],
        "natural_gas":     [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        "district_energy": [1.1, 1.1, 1.2, 1.3, 1.5, 1.6, 1.7, 1.7, 1.5, 1.3, 1.1, 1.0],
        "diesel":          [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    },

    # Tropical — warm and humid year round, minimal seasonal variation
    "tropical": {
        "electricity":     [1.0, 1.0, 1.05, 1.05, 1.1, 1.1, 1.1, 1.1, 1.05, 1.0, 1.0, 1.0],
        "natural_gas":     [1.0, 1.0, 1.0,  1.0,  1.0, 1.0, 1.0, 1.0, 1.0,  1.0, 1.0, 1.0],
        "district_energy": [1.0, 1.0, 1.05, 1.05, 1.1, 1.1, 1.1, 1.1, 1.05, 1.0, 1.0, 1.0],
        "diesel":          [1.0, 1.0, 1.0,  1.0,  1.0, 1.0, 1.0, 1.0, 1.0,  1.0, 1.0, 1.0],
    },

    # Southern hemisphere temperate — seasons are reversed
    "southern_temperate": {
        "electricity":     [1.15, 1.15, 1.0, 0.9, 0.85, 0.9, 1.0, 1.1, 1.1, 1.0, 0.9, 1.1],
        "natural_gas":     [0.2,  0.2,  0.5, 0.9, 1.3,  1.6, 1.5, 1.2, 0.8, 0.5, 0.3, 0.2],
        "district_energy": [0.3,  0.3,  0.6, 0.9, 1.2,  1.5, 1.4, 1.1, 0.8, 0.6, 0.4, 0.3],
        "diesel":          [1.0,  1.0,  1.0, 1.0, 1.1,  1.2, 1.1, 1.0, 1.0, 1.0, 1.0, 1.0],
    },
}

# ── Climate Zone by Country ────────────────────────────────────────────────
CLIMATE_ZONE_BY_COUNTRY = {
    "United States":          "northern_temperate",
    "United Kingdom":         "northern_temperate",
    "France":                 "northern_temperate",
    "Germany":                "northern_temperate",
    "Canada":                 "northern_temperate",
    "Japan":                  "northern_temperate",
    "Australia":              "southern_temperate",
    "United Arab Emirates":   "hot_arid",
    "Singapore":              "tropical",
    "Brazil":                 "southern_temperate",
}
