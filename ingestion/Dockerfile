FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3.3.4

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pyspark==3.5.0

COPY . /app/

RUN mkdir -p /data/bronze /data/raw/csv

CMD ["python", "-m", "run_ingestion"]
