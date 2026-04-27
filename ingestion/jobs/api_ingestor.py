import requests
from ingestion.base.base_ingestor import BaseIngestor, BronzeConfig
from pyspark.sql import SparkSession, DataFrame

class ApiIngestor(BaseIngestor):
    def __init__(self, 
                 spark:SparkSession, 
                 config:BronzeConfig,
                 base_url:str, 
                 endpoint:str, 
                 page_size:int = 500):
        super().__init__(spark, config)
        self.base_url       = base_url
        self.endpoint       = endpoint
        self.page_size      = page_size
    
    def _fetch_page(self, page:int) -> dict:
        params = {
            "page": page, 
            "page_size": self.page_size
        }
        try:
            response = requests.get(f"{self.base_url}{self.endpoint}", params=params)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, list):
                return payload
            return payload.get("data", [])
        except Exception as e:
            raise
            
    def _paginate(self):
        page_no = 1
        flat = []
        run = True
        while run:
            records = self._fetch_page(page_no)
            records = self._fetch_page(page_no)
            if not records:
                break
            flat.extend(records)
            if len(records) < self.page_size:
                break
            page_no += 1
        return [value for d in flat for value in d.values()]

         
    
    def extract(self) -> DataFrame:
        return self.spark.createDataFrame(self._paginate())