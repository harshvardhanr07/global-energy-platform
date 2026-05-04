# spark_session.py
# Factory function that builds a SparkSession configured for
# local Docker mode. All ingestion jobs call get_spark() to
# obtain their session instead of creating one directly.

from pyspark.sql import SparkSession


def get_spark(app_name: str = "GlobalEnergyPlatform") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")                 # use all available cores in Docker
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # faster serialisation
        .config("spark.sql.parquet.compression.codec", "snappy")                   # compressed Parquet output
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.shuffle.partitions", "4")                               # low value tuned for local mode
        .config("spark.driver.memory", "2g")                                       # memory cap for Docker container
        .config("spark.sql.adaptive.enabled", "true")                              # let Spark optimise query plans
        .getOrCreate()                      # reuse existing session if one already exists
    )