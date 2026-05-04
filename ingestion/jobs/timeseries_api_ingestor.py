from ingestion.base.base_ingestor import BaseIngestor, BronzeConfig
from pyspark.sql import SparkSession, DataFrame

class TimeSeriesApiIngestor():

    def __init__(self, spark, config, base_url, endpoint_name, site_ids, db_conn, start_month="2024-04"):
        self.spark = spark
        self.config = config
        self.base_url = base_url
        self.endpoint_name = endpoint_name
        self.site_ids = site_ids
        self.db_conn = db_conn
        self.start_month = start_month
    
   

