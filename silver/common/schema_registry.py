"""
schema_registry.py
Defines the Silver-layer target schema for every table.
All jobs import from here — never define schemas inline.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    DateType, TimestampType, BooleanType, ShortType
)

# ── CSV ──────────────────────────────────────────────────────────────────────

INVOICES = StructType([
    StructField("site_id",                    StringType(),    nullable=False),
    StructField("site_name",                  StringType(),    nullable=True),
    StructField("billing_period_from",        DateType(),      nullable=False),
    StructField("billing_period_to",          DateType(),      nullable=False),
    StructField("consumption_type",           StringType(),    nullable=False),
    StructField("consumption",                DoubleType(),    nullable=True),
    StructField("consumption_unit",           StringType(),    nullable=True),
    StructField("consumption_cost",           DoubleType(),    nullable=True),
    StructField("consumption_cost_unit",      StringType(),    nullable=True),
    StructField("cost_per_consumption_unit",  DoubleType(),    nullable=True),
    StructField("_ingested_at",               TimestampType(), nullable=True),
    StructField("_silver_processed_at",       TimestampType(), nullable=True),
])

# ── API ──────────────────────────────────────────────────────────────────────

SITES = StructType([
    StructField("site_id",       StringType(),    nullable=False),
    StructField("site_name",     StringType(),    nullable=True),
    StructField("country",       StringType(),    nullable=True),
    StructField("city",          StringType(),    nullable=True),
    StructField("latitude",      DoubleType(),    nullable=True),
    StructField("longitude",     DoubleType(),    nullable=True),
    StructField("site_sqm",      IntegerType(),   nullable=True),
    StructField("site_capacity", IntegerType(),   nullable=True),
    StructField("status",        StringType(),    nullable=True),
    StructField("billing_cycle", StringType(),    nullable=True),
    StructField("active_date",   DateType(),      nullable=True),
    StructField("inactive_date", DateType(),      nullable=True),
    StructField("_ingested_at",          TimestampType(), nullable=True),
    StructField("_silver_processed_at",  TimestampType(), nullable=True),
])

CONSUMPTION = StructType([
    StructField("site_id",       StringType(),    nullable=False),
    StructField("timestamp",     TimestampType(), nullable=False),
    StructField("heating",       DoubleType(),    nullable=True),
    StructField("cooling",       DoubleType(),    nullable=True),
    StructField("lighting",      DoubleType(),    nullable=True),
    StructField("ventilation",   DoubleType(),    nullable=True),
    StructField("ups",           DoubleType(),    nullable=True),
    StructField("it",            DoubleType(),    nullable=True),
    StructField("restaurant",    DoubleType(),    nullable=True),
    StructField("_ingested_at",          TimestampType(), nullable=True),
    StructField("_silver_processed_at",  TimestampType(), nullable=True),
])

TEMPERATURE = StructType([
    StructField("site_id",             StringType(),    nullable=False),
    StructField("timestamp",           TimestampType(), nullable=False),
    StructField("avg_outside_temp",    DoubleType(),    nullable=True),
    StructField("degree_day_cooling",  DoubleType(),    nullable=True),
    StructField("degree_day_heating",  DoubleType(),    nullable=True),
    StructField("reference_temp",      DoubleType(),    nullable=True),
    StructField("_ingested_at",          TimestampType(), nullable=True),
    StructField("_silver_processed_at",  TimestampType(), nullable=True),
])

# ── DB ───────────────────────────────────────────────────────────────────────

SITE_PROFILE = StructType([
    StructField("site_id",        StringType(),    nullable=False),
    StructField("site_name",      StringType(),    nullable=True),
    StructField("country",        StringType(),    nullable=True),
    StructField("city",           StringType(),    nullable=True),
    StructField("latitude",       DoubleType(),    nullable=True),
    StructField("longitude",      DoubleType(),    nullable=True),
    StructField("site_sqm",       IntegerType(),   nullable=True),
    StructField("site_capacity",  IntegerType(),   nullable=True),
    StructField("status",         StringType(),    nullable=True),
    StructField("billing_cycle",  StringType(),    nullable=True),
    StructField("active_date",    DateType(),      nullable=True),
    StructField("inactive_date",  DateType(),      nullable=True),
    StructField("last_updated",   TimestampType(), nullable=True),
    StructField("_ingested_at",          TimestampType(), nullable=True),
    StructField("_silver_processed_at",  TimestampType(), nullable=True),
])

SITE_OCCUPANCY = StructType([
    StructField("site_id",        StringType(),  nullable=False),
    StructField("date",           DateType(),    nullable=False),
    StructField("site_capacity",  IntegerType(), nullable=True),
    StructField("occupancy",      IntegerType(), nullable=True),
    StructField("occupancy_pct",  DoubleType(),  nullable=True),   # derived
    StructField("_ingested_at",          TimestampType(), nullable=True),
    StructField("_silver_processed_at",  TimestampType(), nullable=True),
])

SITE_PROFILE_HISTORY = StructType([
    StructField("site_id",      StringType(),    nullable=False),
    StructField("change_field", StringType(),    nullable=True),
    StructField("old_value",    StringType(),    nullable=True),
    StructField("new_value",    StringType(),    nullable=True),
    StructField("changed_on",   TimestampType(), nullable=True),
    StructField("_ingested_at",          TimestampType(), nullable=True),
    StructField("_silver_processed_at",  TimestampType(), nullable=True),
])

SITE_STATUS_HISTORY = StructType([
    StructField("site_id",     StringType(),    nullable=False),
    StructField("event_type",  StringType(),    nullable=True),
    StructField("started_on",  DateType(),      nullable=True),
    StructField("ended_on",    DateType(),      nullable=True),
    StructField("new_site_id", StringType(),    nullable=True),   # nullable — inactive_period has no target
    StructField("_ingested_at",          TimestampType(), nullable=True),
    StructField("_silver_processed_at",  TimestampType(), nullable=True),
])

# ── Registry lookup ──────────────────────────────────────────────────────────

REGISTRY = {
    "csv/invoices":             INVOICES,
    "api/sites":                SITES,
    "api/consumption":          CONSUMPTION,
    "api/temperature":          TEMPERATURE,
    "db/site_profile":          SITE_PROFILE,
    "db/site_occupancy":        SITE_OCCUPANCY,
    "db/site_profile_history":  SITE_PROFILE_HISTORY,
    "db/site_status_history":   SITE_STATUS_HISTORY,
}


def get_schema(table_key: str) -> StructType:
    """Return the Silver StructType for a given table key."""
    if table_key not in REGISTRY:
        raise KeyError(f"No Silver schema registered for '{table_key}'. Keys: {list(REGISTRY)}")
    return REGISTRY[table_key]