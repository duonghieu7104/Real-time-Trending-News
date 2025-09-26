# Dockerfile cho Spark + Jupyter Notebook
FROM jupyter/pyspark-notebook:latest

# Cài đặt Python packages cần thiết - install in batches to avoid timeout
RUN pip install --no-cache-dir --timeout=300 \
    pyspark \
    findspark \
    boto3 \
    kafka-python \
    pymongo

RUN pip install --no-cache-dir --timeout=300 \
    onnxruntime \
    transformers

RUN pip install --no-cache-dir --timeout=300 \
    torch --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir --timeout=300 \
    numpy \
    pandas \
    scikit-learn

# Optional: set timezone hoặc locale nếu cần
ENV TZ=Asia/Ho_Chi_Minh

# Chỉ định thư mục làm việc (giữ mặc định Jupyter Notebook)
WORKDIR /home/jovyan/work
