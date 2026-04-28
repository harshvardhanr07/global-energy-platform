# api_ingestor.py
# Bronze ingestor for the fake_data_platform API simulator.
# Paginates through FastAPI endpoints, collects all records,
# and lands them as raw Parquet in the Bronze layer on S3.

import requests
from base.base_ingestor import BaseIngestor, BronzeConfig
from pyspark.sql import SparkSession, DataFrame


class ApiIngestor(BaseIngestor):

    def __init__(self,
                 spark: SparkSession,
                 config: BronzeConfig,
                 base_url: str,
                 endpoint: str,
                 page_size: int = 500):    # records per API request
        super().__init__(spark, config)
        self.base_url = base_url
        self.endpoint = endpoint
        self.page_size = page_size

    def _fetch_page(self, page: int) -> list:
        # Fetches a single page from the API
        # Supports both bare list [] and {"data": [...]} shaped responses
        params = {
            "page": page,
            "page_size": self.page_size
        }
        response = requests.get(f"{self.base_url}{self.endpoint}", params=params)
        response.raise_for_status()    # raise exception on 4xx/5xx responses
        payload = response.json()
        if isinstance(payload, list):
            return payload
        return payload.get("data", []) # unwrap {"data": [...]} envelope

    def _paginate(self) -> list:
        # Loops through all pages until the API returns an empty page
        # or a page shorter than page_size (signals last page)
        page_no = 1
        flat = []
        while True:
            records = self._fetch_page(page_no)
            if not records:
                break                          # empty response — no more data
            flat.extend(records)
            if len(records) < self.page_size:
                break                          # partial page — last page reached
            page_no += 1
        return flat

    def extract(self) -> DataFrame:
        # Paginate through the full endpoint and convert to Spark DataFrame
        return self.spark.createDataFrame(self._paginate())