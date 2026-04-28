# base_ingestor.py
# Defines the shared building blocks for all Bronze ingestion jobs:
#   - IngestionResult  → dataclass to capture the outcome of each run
#   - BronzeConfig     → dataclass to hold path and write settings
#   - BaseIngestor     → abstract base class all ingestors inherit from

from dataclasses import dataclass
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class IngestionResult:
    # Stores the outcome of a single ingestion run
    # Returned by run() and used by run_ingestion.py to print the summary
    source: str                  # which source: csv, api, or db
    table: str                   # table name e.g. invoices, sites
    rows_written: int            # number of rows written to Bronze
    output_path: str             # full S3 or local path written to
    started_at: datetime         # UTC timestamp when run() was called
    finished_at: datetime        # UTC timestamp when run() completed
    success: bool                # True if write succeeded
    error: str = None            # exception message if success=False


@dataclass
class BronzeConfig:
    # Holds all configuration needed to write a Bronze Parquet table
    # Each ingestor receives one of these at construction time
    bronze_root: str = "/data/bronze"   # root path — set to s3a:// in production
    source_name: str = ""               # source identifier: csv, api, or db
    table_name: str = ""                # output table name e.g. invoices
    partition_by: list = None           # Parquet partition columns
    write_mode: str = "append"          # append | overwrite

    def __post_init__(self):
        # Set default here to avoid mutable default argument issue with dataclasses
        if self.partition_by is None:
            self.partition_by = ["ingestion_date"]


class BaseIngestor(ABC):

    def __init__(self, spark: SparkSession, config: BronzeConfig):
        self.spark = spark
        self.config = config

    @abstractmethod
    def extract(self) -> DataFrame:
        # Subclasses implement this to read from their source
        # and return a raw Spark DataFrame
        ...

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        # Adds three audit columns to every Bronze table:
        #   _ingested_at   → UTC timestamp of when the job ran
        #   _source        → which source system produced this data
        #   ingestion_date → date partition used by Parquet partitionBy
        now = datetime.now(tz=timezone.utc)
        return (
            df
            .withColumn("_ingested_at", F.lit(now.isoformat()))
            .withColumn("_source", F.lit(self.config.source_name))
            .withColumn("ingestion_date", F.lit(now.strftime("%Y-%m-%d")))
        )

    def _output_path(self) -> str:
        # Builds the Bronze output path: bronze_root/source_name/table_name
        # IMPORTANT: uses string join not pathlib.Path — Path collapses
        # s3a://bucket into s3a:/bucket (strips one slash), breaking S3 writes
        return f"{self.config.bronze_root.rstrip('/')}/{self.config.source_name}/{self.config.table_name}"

    def run(self) -> IngestionResult:
        # Orchestrates the full ingestion:
        #   1. extract raw data from source
        #   2. add metadata columns
        #   3. write Parquet to Bronze
        #   4. return IngestionResult with row count and status
        started_at = datetime.now(tz=timezone.utc)
        output_path = self._output_path()
        try:
            df = self.extract()
            df = self._add_metadata(df)
            df.write.mode(self.config.write_mode).partitionBy(*self.config.partition_by).parquet(output_path)
            rows = df.count()
            return IngestionResult(
                source=self.config.source_name,
                table=self.config.table_name,
                rows_written=rows,
                output_path=output_path,
                started_at=started_at,
                finished_at=datetime.now(tz=timezone.utc),
                success=True
            )
        except Exception as e:
            # Catch all exceptions so one failing table doesn't stop the others
            return IngestionResult(
                source=self.config.source_name,
                table=self.config.table_name,
                rows_written=0,
                output_path=output_path,
                started_at=started_at,
                finished_at=datetime.now(tz=timezone.utc),
                success=False,
                error=str(e)
            )