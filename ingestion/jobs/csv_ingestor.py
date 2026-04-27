from ingestion.base.base_ingestor import BaseIngestor, BronzeConfig
from pyspark.sql import SparkSession

class CsvIngestor(BaseIngestor):
    def __init__(self, 
                 spark:SparkSession, 
                 config:BronzeConfig,
                 csv_path:str, 
                 has_header:bool=True, 
                 infer_schema:bool=False):
        super().__init__(spark, config)
        self.csv_path       = csv_path
        self.has_header     = has_header
        self.infer_schema   = infer_schema
    
    def extract(self):
        return (self.spark.read
                .option("header", self.has_header)
                .option("inferSchema", self.infer_schema)
                .option("multiLine", True)
                .option("escape", '"')
                .csv(self.csv_path))