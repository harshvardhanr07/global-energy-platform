# csv_ingestor.py
# Bronze ingestor for CSV files produced by fake_data_platform's csv_generator.
# Reads one or more CSV files from a path or glob pattern and
# lands them as raw Parquet in the Bronze layer.

from ingestion.base.base_ingestor import BaseIngestor, BronzeConfig
from pyspark.sql import SparkSession, DataFrame


class CsvIngestor(BaseIngestor):

    def __init__(self,
                 spark: SparkSession,
                 config: BronzeConfig,
                 csv_path: str,
                 has_header: bool = True,
                 infer_schema: bool = False):   # False at Bronze — Silver will cast types
        super().__init__(spark, config)
        self.csv_path = csv_path
        self.has_header = has_header
        self.infer_schema = infer_schema

    def extract(self) -> DataFrame:
        # Read CSV files from path or glob pattern
        # All columns land as strings when infer_schema=False
        # which keeps Bronze as a faithful raw copy of the source
        return (
            self.spark.read
            .option("header", self.has_header)
            .option("inferSchema", self.infer_schema)
            .option("multiLine", True)      # handles values containing line breaks
            .option("escape", '"')          # handles quoted fields with commas
            .csv(self.csv_path)
        )