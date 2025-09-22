# Dockerfile cho Spark + Jupyter Notebook
FROM jupyter/pyspark-notebook:latest

# Cài đặt Python packages cần thiết
RUN pip install --no-cache-dir \
    findspark \
    boto3 \
    minio \
    kafka-python

# Optional: set timezone hoặc locale nếu cần
ENV TZ=Asia/Ho_Chi_Minh

# Chỉ định thư mục làm việc (giữ mặc định Jupyter Notebook)
WORKDIR /home/jovyan/work
