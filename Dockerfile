FROM apache/airflow:2.9.3-python3.11


USER root

# Install dependencies for Chrome
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libxi6 \
    libxrandr2 \
    libxss1 \
    libxcursor1 \
    libasound2 \
    fonts-liberation \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libgtk-3-0 \
    libx11-6 \
    libxcb1 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome stable
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb

USER airflow

# Install selenium, webdriver-manager and extra Python libs
RUN pip install --no-cache-dir \
    selenium \
    webdriver-manager \
    minio \
    kafka-python \
    pymongo
