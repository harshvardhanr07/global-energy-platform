from pyspark.sql import SparkSession

def get_spark(app_name:str = "GlobalEnergyPlatform") -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", True)
        .getOrCreate()
    )
