FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

# Chuyển sang user root để cài đặt các gói
USER root

# Cài đặt các phụ thuộc hệ thống
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

# Nâng cấp pip trước, sau đó xóa cache (nếu hỗ trợ) và cài đặt setuptools, wheel
RUN pip3 install --upgrade pip \
    && (pip3 cache purge || true) \
    && pip3 install --upgrade setuptools wheel

# Cài đặt các thư viện Python với phiên bản cụ thể
RUN pip3 install --no-cache-dir huggingface_hub==0.16.4 \
    && pip3 install --no-cache-dir sentence-transformers==2.2.2 \
    && pip3 install --no-cache-dir \
        numpy==1.24.3 \
        pandas==2.0.3 \
        onnxruntime==1.16.0 \
        transformers==4.37.2 \
        torch==2.0.1 \
        pymongo==4.5.0 \
        kafka-python==2.0.2 \
        pyvi==0.1.1 \
        bertopic==0.15.0 \
        umap-learn==0.5.4 \
        hdbscan==0.8.33 \
        scikit-learn==1.3.2 \
    && pip3 list

# Thiết lập biến môi trường cho cache
ENV TRANSFORMERS_CACHE=/opt/spark/work-dir/cache/hf
ENV NUMBA_CACHE_DIR=/opt/spark/work-dir/cache/numba

# Tạo thư mục cache và phân quyền
RUN mkdir -p /opt/spark/work-dir/cache/hf /opt/spark/work-dir/cache/numba \
    && chmod -R 777 /opt/spark/work-dir/cache

# Sao chép các file vào container
COPY processor/ /opt/spark/work-dir/processor/
COPY model/ /opt/spark/work-dir/model/
COPY jars/ /opt/spark/work-dir/jars/
COPY src/ /opt/spark/work-dir/src/
COPY entrypoint.sh /opt/entrypoint.sh
COPY checkpoints/ /opt/spark/work-dir/checkpoints/
COPY models/ /opt/spark/work-dir/models/

# Chuyển đổi định dạng dòng (Windows sang Linux)
RUN sed -i 's/\r$//' /opt/entrypoint.sh

# Thiết lập quyền truy cập
RUN chmod -R 755 /opt/spark/work-dir/processor \
                 /opt/spark/work-dir/model \
                 /opt/spark/work-dir/models \
                 /opt/spark/work-dir/checkpoints \
                 /opt/spark/work-dir/jars \
                 /opt/spark/work-dir/src \
    && chmod +x /opt/entrypoint.sh

# Chuyển về user spark
USER spark

# Thư mục làm việc
WORKDIR /opt/spark/work-dir

# Entrypoint
ENTRYPOINT ["/opt/entrypoint.sh"]