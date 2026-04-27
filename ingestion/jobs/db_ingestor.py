from ingestion.base.base_ingestor import BaseIngestor, BronzeConfig
from pyspark.sql import SparkSession, DataFrame

class DbIngestor(BaseIngestor):
    def __init__(self, 
                spark:SparkSession, 
                config:BronzeConfig,
                jdbc_url:str, 
                db_table:str, 
                db_user:str, 
                db_password :str,
                partition_column :str = None, 
                lower_bound:str = None,  
                upper_bound:str = None, 
                num_partitions:int = 4):
        super().__init__(spark, config)
        self.jdbc_url           = jdbc_url
        self.db_table           = db_table
        self.db_user            = db_user
        self.db_password        = db_password
        self.partition_column   = partition_column
        self.lower_bound        = lower_bound
        self.upper_bound        = upper_bound
        self.num_partitions     = num_partitions
   
    def extract(self) -> DataFrame:
        options = {
            "url"       :self.jdbc_url,
            "driver"    :"org.postgresql.Driver",
            "user"      :self.db_user,
            "password"  :self.db_password,
            "dbtable"   :self.db_table
        }

        if self.partition_column:
            options.update({
                "partitionColumn"   : self.partition_column,
                "lowerBound"       : self.lower_bound,
                "upperBound"       : self.upper_bound,
                "numPartitions"    : self.num_partitions,
            })
        
        return self.spark.read.format("jdbc").options(**options).load()