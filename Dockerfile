FROM apache/airflow:2.7.3-python3.10

USER root

# Install OpenJDK 11 for PySpark compliance
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless procps wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export JAVA_HOME

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
