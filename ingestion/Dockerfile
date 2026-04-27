FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . /app/ingestion/

RUN mkdir -p /data/bronze /data/raw/csv

CMD ["python", "-m", "ingestion.run_ingestion"]