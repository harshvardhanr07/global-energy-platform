# base_ingestor.py
# Defines the shared building blocks for all Bronze ingestion jobs:
#   - IngestionResult  → dataclass to capture the outcome of each run
#   - BronzeConfig     → dataclass to hold path and write settings
#   - BaseIngestor     → abstract base class all ingestors inherit from

from dataclasses import dataclass
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class IngestionResult:
    # Stores the outcome of a single ingestion run
    # Used by run_ingestion.py to print the summary
    source: str
    table: str
    rows_written: int
    output_path: str
    started_at: datetime
    finished_at: datetime
    success: bool
    error: str = None


@dataclass
class BronzeConfig:
    # Holds all configuration needed to write a Bronze Parquet table
    # Each ingestor receives one of these at construction time
    bronze_root: str = "/data/bronze"
    source_name: str = ""
    table_name: str = ""
    partition_by: list = None
    write_mode: str = "append"

    def __post_init__(self):
        # Default partition_by here to avoid mutable default argument issue
        if self.partition_by is None:
            self.partition_by = ["ingestion_date"]


class BaseIngestor(ABC):

    def __init__(self, spark: SparkSession, config: BronzeConfig):
        self.spark = spark
        self.config = config

    @abstractmethod
    def extract(self) -> DataFrame:
        # Subclasses implement this to read from their source
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
        return str(
            Path(self.config.bronze_root) / self.config.source_name / self.config.table_name
        )

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
