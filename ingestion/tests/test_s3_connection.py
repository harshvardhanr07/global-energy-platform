import os
from pyspark.sql import Row
from base.spark_session import get_spark

def test_s3_write_read():
    spark = get_spark("GEP-S3-Test")

    bucket = os.environ["S3_BUCKET"]
    test_path = f"s3a://{bucket}/test/connection_check"

    # Write a tiny dataFrame
    df= spark.createDataFrame([
        Row(id=1, message="S3 connection is ok"),
        Row(id=2, message="parquet write ok")
    ])

    df.write.mode("overwrite").parquet(test_path)
    print(f"write sucessful -> {test_path}")

    # Read it back
    df_read = spark.read.parquet(test_path)
    df_read.show()
    print(f"Read successful - {df_read.count()} rows")

    spark.stop()

if __name__=="__main__":
    test_s3_write_read()