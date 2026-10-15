FROM apache/airflow:3.0.0-python3.11

USER root
# Java runs Spark for the bronze -> silver step
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Local DuckDB warehouse built by dbt (mounted as a named volume)
RUN mkdir -p /opt/grontia/warehouse && chown -R airflow: /opt/grontia

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# PySpark + dbt live in their own venv so they never fight Airflow's pins
COPY requirements-pipeline.txt /requirements-pipeline.txt
RUN python -m venv /home/airflow/pipeline-venv \
    && /home/airflow/pipeline-venv/bin/pip install --no-cache-dir -r /requirements-pipeline.txt

# S3A connector jars baked into the image, so Spark doesn't fetch them on every run
ARG MAVEN=https://repo1.maven.org/maven2
RUN JARS=$(/home/airflow/pipeline-venv/bin/python -c "import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
    && curl -fsSL -o "$JARS/hadoop-aws-3.3.4.jar" "$MAVEN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    && curl -fsSL -o "$JARS/aws-java-sdk-bundle-1.12.262.jar" "$MAVEN/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
