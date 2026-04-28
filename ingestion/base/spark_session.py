# spark_session.py
# Factory function that builds or reuses a SparkSession.
# All ingestion jobs call get_spark() rather than creating sessions directly.
# Configured for:
#   - local Docker mode (single node, all cores)
#   - AWS S3 writes via s3a:// using hadoop-aws JARs
#   - PostgreSQL JDBC reads via spark.jars.packages

import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "GlobalEnergyPlatform") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")                  # use all available cores in Docker

        # ── Serialisation & compression ──────────────────────────────────────
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # faster than Java default
        .config("spark.sql.parquet.compression.codec", "snappy")                   # compressed Parquet output

        # ── Local mode tuning ────────────────────────────────────────────────
        .config("spark.sql.shuffle.partitions", "4")   # low value for single-node Docker
        .config("spark.driver.memory", "2g")           # memory cap for Docker container
        .config("spark.sql.adaptive.enabled", "true")  # let Spark optimise query plans

        # ── PostgreSQL JDBC driver ────────────────────────────────────────────
        # Downloaded by Spark at session start via Maven Central
        # Required for db_ingestor.py JDBC reads
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")

        # ── AWS S3 connectivity ───────────────────────────────────────────────
        # Credentials injected from environment variables via docker-compose
        # hadoop-aws and aws-java-sdk JARs are pre-downloaded in the Dockerfile
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        )

        .getOrCreate()  # reuse existing session if one already exists
    )