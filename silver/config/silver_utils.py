from schema_registry import CONSUMPTION
from datetime import datetime



def enforce_shcema(df, schema):
    """
     cast every column to its declared type, drop columns not in schema, add nulls for missing columns
    """
    print(schema)


def deduplicate(df, keys, order_col):
    """window over keys ordered by order_col descending, keep row_number == 1"""


def normalize_strings(df, cols):
    """trim whitespace and lowercase on specified string columns"""

def add_silver_metadata(df, source_table):
    """append silver_processed_at (current timestamp), silver_source (table name string)"""

def unix_to_timestamp(df, unix_col, output_col):
    """convert integer unix seconds to TimestampType — needed by both consumption and temperature"""