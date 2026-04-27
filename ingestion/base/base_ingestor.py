from dataclasses import dataclass
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

@dataclass
class IngestionResult:
    source:str
    table:str
    rows_written:int
    output_path:str
    started_at:datetime
    finished_at:datetime
    success:bool
    error:str=None

@dataclass
class BronzeConfig:
    bronze_root:str="/data/bronze"
    source_name:str=""
    table_name:str=""
    partition_by:list=["ingestion_date"]
    write_mode:str="append"

class BaseIngestor(ABC):

    def __init__(self, spark, config:BronzeConfig):
        self.spark  = spark
        self.config = config

    @abstractmethod
    def extract(self) -> DataFrame:
        ...

    def _add_metadata(self, df:DataFrame) -> DataFrame:
        now = datetime.now(tz=timezone.utc)
        return (
            df
            .withColumn("_ingested_at", F.lit(now.isoformat()))
            .withColumn("_source", F.lit(self.config.source_name))
            .withColumn("ingestion_date", F.lit(now.strftime("%Y-%m-%d")))
        )
       
    def _output_path(self):
        return str(Path(self.config.bronze_root)/ self.config.source_name/self.config.table_name)

    def run(self) -> IngestionResult:
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
        except  Exception as e:
            return IngestionResult(
                source=self.config.source_name, 
                table=self.config.table_name, 
                rows_written=0, 
                output_path=output_path, 
                started_at=started_at, 
                finished_at=datetime.now(tz=timezone.utc), 
                success=False, 
                error=str(e))
