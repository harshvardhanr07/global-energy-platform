"""
inspect_bronze_schemas.py  —  one-off Bronze schema inspector
Place in: ~/projects/global-energy-platform/ingestion/
Run with: docker compose run --rm -v $(pwd):/app ingestion python inspect_bronze_schemas.py
"""

import os
from pyspark.sql import SparkSession, functions as F

import sys
sys.path.insert(0, "/app")
from base.spark_session import get_spark

spark = get_spark("bronze-schema-inspector")
spark.sparkContext.setLogLevel("WARN")

BRONZE_ROOT = os.getenv("BRONZE_ROOT", "s3a://gep-datalake-dev/bronze")

TABLES = [
    ("csv", "invoices"),
    ("api", "sites"),
    ("db",  "site_profile"),
    ("db",  "site_occupancy"),
    ("db",  "site_profile_history"),
    ("db",  "site_status_history"),
]

SEP = "=" * 70

for source, table in TABLES:
    path = f"{BRONZE_ROOT}/{source}/{table}"
    print(f"\n{SEP}")
    print(f"  TABLE : {source}/{table}")
    print(f"  PATH  : {path}")
    print(SEP)

    try:
        df = spark.read.parquet(path)
        row_count = df.count()

        print(f"\n  SCHEMA  ({row_count:,} rows)")
        for field in df.schema.fields:
            print(f"    {field.name:<40} {str(field.dataType):<25} nullable={field.nullable}")

        null_counts = df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in df.columns
        ]).collect()[0].asDict()

        non_zero = {k: v for k, v in null_counts.items() if v and v > 0}
        if non_zero:
            print(f"\n  NULLS DETECTED:")
            for col_name, cnt in non_zero.items():
                pct = cnt / row_count * 100
                print(f"    {col_name:<40} {cnt:>6} nulls  ({pct:.1f}%)")
        else:
            print(f"\n  NULLS: none")

        print(f"\n  SAMPLE (3 rows):")
        df.show(3, truncate=60)

    except Exception as e:
        print(f"\n  ERROR: {e}")

print(f"\n{SEP}")
print("  Done.")
print(SEP)
spark.stop()