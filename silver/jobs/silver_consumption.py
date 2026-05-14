"""
silver_consumption.py
Reads bronze api/consumption from S3, applies cleaning/DQ/dedup,
writes silver api/consumption back to S3.
Tracks progress via silver_watermark and silver_job_log in PostgreSQL.
"""

import logging
import os
from datetime import datetime, timezone

import psycopg2
from pyspark.sql import SparkSession, functions as F

from common.schema_registry import get_schema
from common.silver_utils import (
    enforce_schema,
    deduplicate,
    normalize_strings,
    add_silver_metadata,
    unix_to_timestamp,
)
from common.quality_checks import (
    run_checks,
    assert_no_nulls,
    assert_range,
    assert_unique,
)

logger = logging.getLogger(__name__)

TABLE_KEY   = "api/consumption"
ENERGY_COLS = ["heating", "cooling", "lighting", "ventilation", "ups", "it", "restaurant"]


class SilverConsumptionJob:

    def __init__(self, spark: SparkSession, pg_conn, bronze_root: str, silver_root: str):
        self.spark       = spark
        self.pg_conn     = pg_conn
        self.bronze_path = f"{bronze_root}/api/consumption"
        self.silver_path = f"{silver_root}/api/consumption"

    # ── watermark helpers ────────────────────────────────────────────────────

    def _get_watermark(self):
        with self.pg_conn.cursor() as cur:
            cur.execute(
                "SELECT last_silver_ts FROM silver_watermark WHERE table_name = %s",
                (TABLE_KEY,),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def _update_watermark(self, last_ts):
        with self.pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver_watermark (table_name, last_silver_ts, updated_at, status)
                VALUES (%s, %s, NOW(), 'success')
                ON CONFLICT (table_name) DO UPDATE
                SET last_silver_ts = EXCLUDED.last_silver_ts,
                    updated_at     = EXCLUDED.updated_at,
                    status         = EXCLUDED.status
                """,
                (TABLE_KEY, last_ts),
            )
        self.pg_conn.commit()

    # ── job log helper ───────────────────────────────────────────────────────

    def _write_job_log(self, started_at, finished_at, rows_read, rows_written, rows_rejected, status, error=None):
        with self.pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver_job_log
                    (table_name, started_at, finished_at,
                     rows_read, rows_written, rows_rejected, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (TABLE_KEY, started_at, finished_at,
                 rows_read, rows_written, rows_rejected, status, error),
            )
        self.pg_conn.commit()

    # ── main ─────────────────────────────────────────────────────────────────

    def run(self):
        started_at = datetime.now(timezone.utc)
        logger.info("[%s] starting silver job", TABLE_KEY)

        try:
            # 1. read bronze
            df = self.spark.read.parquet(self.bronze_path)
            rows_read = df.count()
            logger.info("[%s] bronze rows read: %d", TABLE_KEY, rows_read)

            # 2. watermark filter
            watermark_ts = self._get_watermark()
            if watermark_ts:
                logger.info("[%s] incremental — filtering rows after %s", TABLE_KEY, watermark_ts)
                df = df.filter(F.col("timestamp") > watermark_ts.timestamp())
            else:
                logger.info("[%s] no watermark — full load", TABLE_KEY)

            if df.rdd.isEmpty():
                logger.info("[%s] no new rows to process", TABLE_KEY)
                self._write_job_log(started_at, datetime.now(timezone.utc), rows_read, 0, 0, "success")
                return

            # 3. convert unix timestamp → TimestampType
            df = unix_to_timestamp(df, unix_col="timestamp", output_col="timestamp")

            # 4. normalize strings
            df = normalize_strings(df, cols=["site_id"])

            # 5. enforce schema
            df = enforce_schema(df, get_schema(TABLE_KEY))

            # 6. DQ checks
            checks = [
                lambda d: assert_no_nulls(d, ["site_id", "timestamp"], TABLE_KEY),
                *[
                    (lambda col: lambda d: assert_range(d, col, 0, None, TABLE_KEY))(c)
                    for c in ENERGY_COLS
                ],
                lambda d: assert_unique(d, ["site_id", "timestamp"], TABLE_KEY),
            ]
            df, dq_result = run_checks(df, TABLE_KEY, checks)

            # 7. deduplicate
            df = deduplicate(df, keys=["site_id", "timestamp"], order_col="_ingested_at")

            # 8. silver metadata
            df = add_silver_metadata(df)

            # 9. partition columns
            df = (
                df
                .withColumn("year",  F.year(F.col("timestamp")))
                .withColumn("month", F.month(F.col("timestamp")))
            )

            # 10. write to silver
            (
                df.write
                .mode("overwrite")
                .option("partitionOverwriteMode", "dynamic")
                .partitionBy("year", "month")
                .parquet(self.silver_path)
            )
            rows_written = dq_result.rows_passed
            logger.info("[%s] rows written to silver: %d", TABLE_KEY, rows_written)

            # 11. update watermark
            max_ts = df.agg(F.max("timestamp")).collect()[0][0]
            self._update_watermark(max_ts)

            # 12. write job log
            finished_at = datetime.now(timezone.utc)
            self._write_job_log(
                started_at, finished_at,
                rows_read, rows_written, dq_result.rows_rejected,
                "success",
            )
            logger.info(dq_result.summary())

        except Exception as exc:
            logger.exception("[%s] job failed: %s", TABLE_KEY, exc)
            self._write_job_log(
                started_at, datetime.now(timezone.utc),
                0, 0, 0, "failed", str(exc),
            )
            raise
