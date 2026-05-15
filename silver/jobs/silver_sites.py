"""
silver_sites.py
Reads bronze api/sites from S3, applies cleaning/DQ/dedup,
writes silver api/sites back to S3.
Full overwrite every run — sites is a small static table.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

from common.schema_registry import get_schema
from common.silver_utils import (
    enforce_schema,
    deduplicate,
    normalize_strings,
    add_silver_metadata,
)
from common.quality_checks import (
    run_checks,
    assert_no_nulls,
    assert_range,
    assert_unique,
)

logger = logging.getLogger(__name__)

TABLE_KEY = "api/sites"


class SilverSitesJob:

    def __init__(self, spark: SparkSession, pg_conn, bronze_root: str, silver_root: str):
        self.spark       = spark
        self.pg_conn     = pg_conn
        self.bronze_path = f"{bronze_root}/api/sites"
        self.silver_path = f"{silver_root}/api/sites"

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

            # 2. normalize strings
            df = normalize_strings(df, cols=["site_id", "site_name", "country", "city", "status", "billing_cycle"])

            # 3. enforce schema
            df = enforce_schema(df, get_schema(TABLE_KEY))

            # 4. DQ checks
            checks = [
                lambda d: assert_no_nulls(d, ["site_id"], TABLE_KEY),
                lambda d: assert_range(d, "latitude",  -90,  90, TABLE_KEY),
                lambda d: assert_range(d, "longitude", -180, 180, TABLE_KEY),
                lambda d: assert_unique(d, ["site_id"], TABLE_KEY),
            ]
            df, dq_result = run_checks(df, TABLE_KEY, checks)

            # 5. deduplicate — keep latest per site_id
            df = deduplicate(df, keys=["site_id"], order_col="_ingested_at")

            # 6. silver metadata
            df = add_silver_metadata(df)

            # 7. write to silver — full overwrite
            (
                df.write
                .mode("overwrite")
                .parquet(self.silver_path)
            )
            rows_written = dq_result.rows_passed
            logger.info("[%s] rows written to silver: %d", TABLE_KEY, rows_written)

            # 8. write job log
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