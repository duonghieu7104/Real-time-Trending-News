FROM apache/airflow:2.6.3-python3.11

# Install OS utilities required by DAG helper commands (e.g., pgrep)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    psmisc \
    && rm -rf /var/lib/apt/lists/*

# Install only essential Python packages for Airflow as airflow user
USER airflow
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.7 \
    pymongo==4.5.0 \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    feedparser==6.0.10 \
    beautifulsoup4==4.12.2

# Set working directory
WORKDIR /opt/airflow
