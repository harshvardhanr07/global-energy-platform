"""
silver_utils.py
Shared helpers used by every Silver transformer.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Cast every column in the DataFrame to the type declared in the schema.
    Columns present in schema but missing from df are added as nulls.
    Columns present in df but not in schema are dropped.
    """
    schema_cols = {field.name: field.dataType for field in schema.fields}
    df_cols = set(df.columns)

    select_exprs = []
    for field in schema.fields:
        if field.name in df_cols:
            select_exprs.append(
                F.col(field.name).cast(field.dataType).alias(field.name)
            )
        else:
            select_exprs.append(
                F.lit(None).cast(field.dataType).alias(field.name)
            )

    return df.select(select_exprs)


def deduplicate(df: DataFrame, keys: list, order_col: str, descending: bool = True) -> DataFrame:
    """
    Keep one row per unique key combination, ordered by order_col.
    Default: keep the latest row (descending=True).
    """
    from pyspark.sql.window import Window

    order = F.col(order_col).desc() if descending else F.col(order_col).asc()
    window = Window.partitionBy(*keys).orderBy(order)

    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def normalize_strings(df: DataFrame, cols: list) -> DataFrame:
    """
    Trim whitespace and lowercase a list of string columns.
    Leaves nulls as nulls.
    """
    for col in cols:
        df = df.withColumn(col, F.trim(F.lower(F.col(col))))
    return df


def drop_columns(df: DataFrame, cols: list) -> DataFrame:
    """Drop a list of columns — ignores any that don't exist."""
    existing = [c for c in cols if c in df.columns]
    return df.drop(*existing)


def add_silver_metadata(df: DataFrame) -> DataFrame:
    """Append _silver_processed_at timestamp to every Silver row."""
    now = datetime.now(timezone.utc)
    return df.withColumn("_silver_processed_at", F.lit(now).cast("timestamp"))