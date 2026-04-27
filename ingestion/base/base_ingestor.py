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
    bronze_root: str = "/data/bronze"       # root folder for all Bronze output
    source_name: str = ""                    # e.g. "csv", "api", "db"
    table_name: str = ""                     # e.g. "energy_readings"
    partition_by: list = None                # partition columns for Parquet
    write_mode: str = "append"               # append | overwrite

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
        # Subclass