FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

# Switch to root to install packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    build-essential \
    gcc \
    gfortran \
    libopenblas-dev \
    liblapack-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip3 install --upgrade pip setuptools wheel

# Install base Python packages for ONNX processing
RUN pip3 install --no-cache-dir \
    numpy==1.24.3 \
    pandas==2.0.3 \
    onnxruntime==1.16.0 \
    transformers==4.33.0 \
    torch==2.0.1 \
    pymongo==4.5.0 \
    kafka-python==2.0.2 \
    pyvi==0.1.1 \
    bertopic==0.15.0 \
    umap-learn==0.5.4 \
    hdbscan==0.8.33 \
    scikit-learn==1.3.2



# Copy our files
COPY processor/ /opt/spark/work-dir/processor/
COPY model/ /opt/spark/work-dir/model/
COPY jars/ /opt/spark/work-dir/jars/
COPY src/ /opt/spark/work-dir/src/
COPY entrypoint.sh /opt/entrypoint.sh
COPY checkpoints/ /opt/spark/work-dir/checkpoints/
COPY models/ /opt/spark/work-dir/models/


# Convert line endings (Windows to Linux)
RUN sed -i 's/\r$//' /opt/entrypoint.sh

# Set proper permissions
RUN chmod -R 755 /opt/spark/work-dir/processor \
                 /opt/spark/work-dir/model \
                 /opt/spark/work-dir/models \
                 /opt/spark/work-dir/checkpoints \
                 /opt/spark/work-dir/jars \
                 /opt/spark/work-dir/src 
                 
RUN chmod +x /opt/entrypoint.sh

# Switch back to spark user
USER spark

# Set working directory
WORKDIR /opt/spark/work-dir

# Set entrypoint
ENTRYPOINT ["/opt/entrypoint.sh"]