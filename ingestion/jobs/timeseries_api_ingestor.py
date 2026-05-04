import requests
from datetime import datetime, timezone
from calendar import monthrange
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from base.base_ingestor import BronzeConfig
from base.watermark import WatermarkManager
from base.ingestion_log import IngestionLogger
from base.base_ingestor import BronzeConfig, IngestionResult

class TimeSeriesApiIngestor:

    def __init__(
        self,
        spark: SparkSession,
        config: BronzeConfig,
        base_url: str,
        endpoint_name: str,
        site_ids: list,
        db_conn,
        start_ts: datetime = datetime(2024, 4, 1, 0, 0, 0),
    ):
        self.spark = spark
        self.config = config
        self.base_url = base_url
        self.endpoint_name = endpoint_name
        self.site_ids = site_ids
        self.db_conn = db_conn
        self.start_ts = start_ts
        self.watermark = WatermarkManager(db_conn)
        self.logger = IngestionLogger(db_conn)

    def _get_months_to_fetch(self, site_id: str) -> list:
        """
        Returns a list of (from_ts, to_ts) datetime tuples — one per full calendar month —
        starting from the month after last_ingested_ts up to and including the current month.
        """
        last_ingested_ts = self.watermark.get(self.endpoint_name, site_id)

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        current_year = now.year
        current_month = now.month

        # Start from the month of last_ingested_ts (we re-fetch the current partial month)
        start_year = last_ingested_ts.year
        start_month = last_ingested_ts.month

        months = []
        year = start_year
        month = start_month

        while (year, month) <= (current_year, current_month):
            month_start = datetime(year, month, 1, 0, 0, 0)
            last_day = monthrange(year, month)[1]
            month_end = datetime(year, month, last_day, 23, 59, 59)

            # Only fetch from where we left off within the first month
            from_ts = last_ingested_ts if (year == start_year and month == start_month) else month_start

            months.append((from_ts, month_end))

            # Advance to next month
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

        return months

    def _fetch_month(self, site_id: str, from_ts: datetime, to_ts: datetime) -> list:
        """
        Fetches data from the API for a given site and time range.
        from_ts and to_ts are converted to Unix integer timestamps.
        Raises an exception if the response is not 200.
        """
        from_unix = int(from_ts.replace(tzinfo=timezone.utc).timestamp())
        to_unix = int(to_ts.replace(tzinfo=timezone.utc).timestamp())

        url = f"{self.base_url}/site/{site_id}/{self.endpoint_name}"
        response = requests.get(url, params={"from_ts": from_unix, "to_ts": to_unix})

        if response.status_code != 200:
            raise Exception(
                f"API error for {site_id} [{self.endpoint_name}] "
                f"{from_ts} -> {to_ts}: HTTP {response.status_code} — {response.text}"
            )

        return response.json().get("data", [])

    def _to_dataframe(self, site_id: str, data: list):
        """
        Converts the API data list to a Spark DataFrame.
        Adds site_id, _ingested_at, _source, and ingestion_date metadata columns.
        """
        if not data:
            return None

        ingested_at = datetime.now(timezone.utc).isoformat()

        # Add site_id to each record before creating the DataFrame
        enriched = [{**row, "site_id": site_id} for row in data]

        df = self.spark.createDataFrame(enriched)
        df = (
            df
            .withColumn("_ingested_at", F.lit(ingested_at))
            .withColumn("_source", F.lit("api"))
            .withColumn("ingestion_date", F.current_date())
            .withColumn(
                "year_month",
                F.date_format(
                    F.to_timestamp(F.col("timestamp")),
                    "yyyy-MM"
                )
            )
        )

        return df

   def run(self) -> list:
    output_path = f"{self.config.bronze_root}/api/{self.endpoint_name}"
    all_results = []

    for site_id in self.site_ids:
        months = self._get_months_to_fetch(site_id)

        for from_ts, to_ts in months:
            year_month = from_ts.strftime("%Y-%m")
            started_at = datetime.now(timezone.utc).replace(tzinfo=None)

            try:
                data = self._fetch_month(site_id, from_ts, to_ts)
                df = self._to_dataframe(site_id, data)
                rows_written = 0

                if df is not None:
                    df.write.mode("overwrite").partitionBy("year_month", "site_id").parquet(output_path)
                    rows_written = df.count()

                finished_at = datetime.now(timezone.utc).replace(tzinfo=None)

                self.watermark.update(self.endpoint_name, site_id, to_ts)
                self.logger.log(self.endpoint_name, site_id, year_month, rows_written, "success", None, started_at, finished_at)

                all_results.append(IngestionResult(
                    source="api",
                    table=f"{self.endpoint_name}/{site_id}/{year_month}",
                    rows_written=rows_written,
                    output_path=output_path,
                    started_at=started_at,
                    finished_at=finished_at,
                    success=True,
                ))

                print(f"  ✓ {self.endpoint_name} | {site_id} | {year_month} | {rows_written:,} rows")

            except Exception as e:
                finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
                self.logger.log(self.endpoint_name, site_id, year_month, 0, "failed", str(e), started_at, finished_at)

                all_results.append(IngestionResult(
                    source="api",
                    table=f"{self.endpoint_name}/{site_id}/{year_month}",
                    rows_written=0,
                    output_path=output_path,
                    started_at=started_at,
                    finished_at=finished_at,
                    success=False,
                    error=str(e),
                ))

                print(f"  ✗ {self.endpoint_name} | {site_id} | {year_month} | ERROR: {e}")

    return all_results