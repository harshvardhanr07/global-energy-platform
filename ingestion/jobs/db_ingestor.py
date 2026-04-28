# db_ingestor.py
# Bronze ingestor for PostgreSQL tables seeded by fake_data_platform's db_seeder.
# Uses PySpark JDBC to pull tables from PostgreSQL and land them
# as raw Parquet in the Bronze layer on S3.
# Supports parallel reads via partition_column for large tables.

from base.base_ingestor import BaseIngestor, BronzeConfig
from pyspark.sql import SparkSession, DataFrame


class DbIngestor(BaseIngestor):

    def __init__(self,
                 spark: SparkSession,
                 config: BronzeConfig,
                 jdbc_url: str,
                 db_table: str,
                 db_user: str,
                 db_password: str,
                 partition_column: str = None,  # numeric/date column to split parallel reads
                 lower_bound: str = None,        # min value of partition_column
                 upper_bound: str = None,        # max value of partition_column
                 num_partitions: int = 4):       # number of parallel JDBC tasks
        super().__init__(spark, config)
        self.jdbc_url = jdbc_url
        self.db_table = db_table
        self.db_user = db_user
        self.db_password = db_password
        self.partition_column = partition_column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.num_partitions = num_partitions

    def extract(self) -> DataFrame:
        # Build JDBC options dict — always includes connection details
        options = {
            "url":      self.jdbc_url,
            "driver":   "org.postgresql.Driver",    # PostgreSQL JDBC driver
            "user":     self.db_user,
            "password": self.db_password,
            "dbtable":  self.db_table
        }

        # If partition_column is set, add parallel read hints
        # Spark splits the read into num_partitions tasks using the column range
        if self.partition_column:
            options.update({
                "partitionColumn":  self.partition_column,
                "lowerBound":       self.lower_bound,
                "upperBound":       self.upper_bound,
                "numPartitions":    self.num_partitions,
            })

        return self.spark.read.format("jdbc").options(**options).load()