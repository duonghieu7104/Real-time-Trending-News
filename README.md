
# Real-time Trending News System

Hệ thống thu thập và xử lý tin tức real-time sử dụng Apache Airflow, Spark, Kafka, MongoDB và Elasticsearch.

## 🏗️ Architecture

- **Airflow**: Orchestration và crawling
- **Apache Spark**: Xử lý ONNX embeddings (thay thế Jupyter)
- **Kafka**: Message streaming
- **MongoDB**: Data storage
- **Elasticsearch**: Search và analytics

## 🚀 Quick Start

### 1. Build và chạy containers
```bash
# Windows
docker-compose up --build -d

# Linux/Mac
./build-and-run.sh
```

### 2. Access URLs
- **Airflow UI**: http://localhost:8085 (admin/admin)
- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081
- **Elasticsearch**: http://localhost:9200
- **Kibana**: http://localhost:5601

## 📋 Services

### Airflow DAGs
- `kenh14_producer_dag`: Crawl Kenh14 RSS feeds
- `vnexpress_producer_dag`: Crawl VnExpress RSS feeds
- `rss_consumer_dag`: Continuous RSS consumer
- `spark_embedding_processing_dag`: Spark job cho ONNX embeddings

### Spark Jobs
- `spark_onnx_processor.py`: Xử lý ONNX embeddings trên Spark cluster

## 🔧 Requirements Files

- `requirements-airflow.txt`: Dependencies cho Airflow (không có torch)
- `requirements-spark.txt`: Dependencies cho Spark (có torch, onnx)

## 📊 Data Flow

1. **Crawling**: Airflow DAGs crawl RSS feeds
2. **Streaming**: Data được gửi qua Kafka topic `raw_news`
3. **Storage**: MongoDB lưu trữ raw data
4. **Processing**: Spark job xử lý ONNX embeddings
5. **Search**: Elasticsearch index cho search

## 🛠️ Manual Commands

```bash
# Run crawler manually
docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/Kenh14_Crawler.py

# Run RSS consumer manually  
docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/RSS_Consumer.py

# Trigger Spark embedding processing
docker exec -it airflow-webserver-v4 airflow dags trigger spark_embedding_processing_dag
```