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

# Download S3 connector JARs into the correct PySpark jars directory
RUN PYSPARK_JARS=$(python -c "import pyspark; print(pyspark.__file__.replace('__init__.py', 'jars'))") && \
    curl -o ${PYSPARK_JARS}/hadoop-aws-${HADOOP_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -o ${PYSPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY . /app/

RUN mkdir -p /data/bronze /data/raw/csv

CMD ["python", "-m", "run_ingestion"]