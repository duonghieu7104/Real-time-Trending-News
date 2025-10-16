from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired
from transformers import pipeline
from elasticsearch import Elasticsearch, helpers
from sklearn.cluster import MiniBatchKMeans
import pandas as pd
import numpy as np
import logging
import os
import pickle
from datetime import datetime
from collections import deque

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------------------
# Spark Configuration
# -------------------------------
spark = (
    SparkSession.builder
        .appName("Online-Training-BERTopic-Sentiment")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
)

schema = StructType([
    StructField("_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("content", StringType(), True),
    StructField("embedding", ArrayType(FloatType()), True),
    StructField("source", StringType(), True),
    StructField("category", StringType(), True),
    StructField("published_at", StringType(), True)
])

# -------------------------------
# Global Variables cho Online Learning
# -------------------------------
MODEL_PATH = "/models/online_bertopic"
UPDATE_FREQUENCY = 100  # Cập nhật model sau mỗi 100 documents
MAX_BUFFER_SIZE = 1000  # Buffer tối đa
MIN_DOCS_FOR_TRAINING = 50  # Số docs tối thiểu để train lần đầu

# Buffer để tích lũy documents
doc_buffer = deque(maxlen=MAX_BUFFER_SIZE)
embedding_buffer = deque(maxlen=MAX_BUFFER_SIZE)
processed_count = 0
last_update_count = 0

# Load hoặc khởi tạo BERTopic model
if os.path.exists(MODEL_PATH):
    logger.info(f"Loading existing model from {MODEL_PATH}")
    topic_model = BERTopic.load(MODEL_PATH)
    model_exists = True
else:
    logger.info("Creating new BERTopic model with MiniBatchKMeans for online learning")
    # Sử dụng MiniBatchKMeans để có thể cập nhật tăng dần
    topic_model = BERTopic(
        embedding_model=None,  # Dùng embedding có sẵn
        hdbscan_model=MiniBatchKMeans(n_clusters=20, random_state=42, batch_size=100),
        verbose=False,
        calculate_probabilities=False,  # Tắt để nhanh hơn
        nr_topics="auto"
    )
    model_exists = False

# Khởi tạo Sentiment Pipeline (load 1 lần)
logger.info("Loading sentiment analysis model...")
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-xlm-roberta-base-sentiment",
    device=-1,  # CPU
    truncation=True,
    max_length=512
)

# Khởi tạo Elasticsearch client (load 1 lần)
logger.info("Connecting to Elasticsearch...")
es_client = Elasticsearch(
    ["http://elasticsearch:9200"],
    retry_on_timeout=True,
    max_retries=3,
    request_timeout=30
)

# Kiểm tra kết nối ES
try:
    res = es_client.info()   # Dùng GET / thay vì HEAD /
    print("✅ Connected to Elasticsearch:", res["cluster_name"])
except Exception as e:
    print("❌ Elasticsearch connection failed:", e)

# -------------------------------
# Read from Kafka
# -------------------------------
df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka-v4:29092")
        .option("subscribe", "processed_data")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "50")  # Batch size nhỏ hơn cho online learning
        .option("failOnDataLoss", "false")
        .load()
)

parsed_df = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
      .select("data.*")
)

# -------------------------------
# Batch Processing với Online Learning
# -------------------------------
# Xử lý từng batch
batch_results = []

def process_batch(batch_df, batch_id):
    logger.info(f"========== Processing Batch {batch_id} ==========")

    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id} is empty, skipping...")
        return

    try:
        # Chuyển sang Pandas
        pdf = batch_df.toPandas()
        logger.info(f"Batch {batch_id}: {len(pdf)} documents")
        
        # Chuẩn bị text và embeddings
        texts = (pdf["content"].fillna("") + " " + pdf["title"].fillna("")).tolist()
        embeddings = [np.array(emb, dtype=np.float32) for emb in pdf["embedding"].tolist()]
        
        # Thêm vào buffer
        doc_buffer.extend(texts)
        embedding_buffer.extend(embeddings)
        processed_count += len(texts)
        
        logger.info(f"Buffer size: {len(doc_buffer)}/{MAX_BUFFER_SIZE}")
        
        # ===== ONLINE LEARNING LOGIC =====
        
        # TH1: Model chưa được train lần nào
        if not model_exists and len(doc_buffer) >= MIN_DOCS_FOR_TRAINING:
            logger.info(f"🔥 Initial training with {len(doc_buffer)} documents")
            
            buffer_texts = list(doc_buffer)
            buffer_embeddings = list(embedding_buffer)
            
            topics, probs = topic_model.fit_transform(buffer_texts, buffer_embeddings)
            model_exists = True
            last_update_count = processed_count
            
            # Lưu model
            topic_model.save(MODEL_PATH, serialization="pickle", save_ctfidf=True)
            logger.info(f"✅ Model saved to {MODEL_PATH}")
        
        # TH2: Model đã tồn tại, cập nhật incremental
        elif model_exists and (processed_count - last_update_count) >= UPDATE_FREQUENCY:
            logger.info(f"🔄 Incremental update after {processed_count - last_update_count} docs")
            
            # Lấy documents mới từ buffer
            new_docs = list(doc_buffer)[-UPDATE_FREQUENCY:]
            new_embeddings = list(embedding_buffer)[-UPDATE_FREQUENCY:]
            
            # Cập nhật model (partial fit)
            # BERTopic không hỗ trợ partial_fit trực tiếp, nên ta sẽ:
            # 1. Dự đoán topics cho docs mới
            # 2. Merge với topics cũ
            # 3. Retrain nếu cần
            
            topics, probs = topic_model.transform(new_docs, new_embeddings)
            
            # Kiểm tra có topic mới xuất hiện không (-1 = outlier)
            unique_topics = set(topics)
            existing_topics = set(topic_model.get_topic_info()["Topic"].tolist())
            
            # Nếu có nhiều outliers (>20%), retrain với buffer
            outlier_ratio = sum(1 for t in topics if t == -1) / len(topics)
            
            if outlier_ratio > 0.2:
                logger.info(f"⚠️ High outlier ratio ({outlier_ratio:.2%}), retraining...")
                
                buffer_texts = list(doc_buffer)
                buffer_embeddings = list(embedding_buffer)
                
                # Update topics (merge old and new)
                topic_model.update_topics(
                    buffer_texts, 
                    topics=topics,
                    vectorizer_model=topic_model.vectorizer_model
                )
                
                # Lưu model đã cập nhật
                topic_model.save(MODEL_PATH, serialization="pickle", save_ctfidf=True)
                logger.info(f"✅ Model updated and saved")
            
            last_update_count = processed_count
        
        # TH3: Dự đoán với model hiện tại
        if model_exists:
            topics, probs = topic_model.transform(texts, embeddings)
            
            # Lấy thông tin topic
            topic_info = topic_model.get_topics()
            topic_names = []
            topic_keywords = []
            
            for t in topics:
                if t != -1 and t in topic_info:
                    words = [w for w, _ in topic_info[t][:5]]
                    topic_keywords.append(words)
                    topic_names.append(", ".join(words))
                else:
                    topic_keywords.append([])
                    topic_names.append("Outlier/Unknown")
            
            pdf["topic_id"] = topics
            pdf["topic_name"] = topic_names
            pdf["topic_keywords"] = topic_keywords
            pdf["topic_score"] = [float(p) if p is not None else 0.0 for p in probs]
        
        else:
            # Model chưa sẵn sàng
            logger.info("⏳ Waiting for enough documents to train initial model...")
            pdf["topic_id"] = -1
            pdf["topic_name"] = "Pending"
            pdf["topic_keywords"] = [[]]
            pdf["topic_score"] = 0.0
        
        # ===== SENTIMENT ANALYSIS =====
        logger.info("Analyzing sentiment...")
        sentiments = sentiment_pipeline(texts, truncation=True, max_length=512)
        
        pdf["sentiment_label"] = [s["label"] for s in sentiments]
        pdf["sentiment_score"] = [float(s["score"]) for s in sentiments]
        pdf["processed_at"] = datetime.now().isoformat()
        
        # ===== SAVE TO ELASTICSEARCH =====
        logger.info("Indexing to Elasticsearch...")
        
        # Bulk insert (nhanh hơn nhiều so với từng document)
        actions = []
        for _, row in pdf.iterrows():
            doc = row.to_dict()
            
            # Chuyển đổi numpy types sang Python native types
            if isinstance(doc.get("topic_keywords"), list):
                doc["topic_keywords"] = [str(k) for k in doc["topic_keywords"]]
            
            action = {
                "_index": "articles_topic_sentiment_online",
                "_id": doc["_id"],
                "_source": doc
            }
            actions.append(action)
        
        # Bulk insert
        success, failed = helpers.bulk(
            es_client, 
            actions, 
            raise_on_error=False,
            stats_only=False
        )
        
        logger.info(f"✅ Batch {batch_id} completed: {success} indexed, {len(failed)} failed")
        
        # Log thống kê
        logger.info(f"📊 Stats - Total processed: {processed_count}, Buffer: {len(doc_buffer)}, Model exists: {model_exists}")
        
    except Exception as e:
        logger.error(f"❌ Error processing batch {batch_id}: {str(e)}", exc_info=True)


# -------------------------------
# Start Streaming
# -------------------------------
query = (
    parsed_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "/tmp/checkpoints/online_topic_sentiment")
        .trigger(processingTime="30 seconds")
        .start()
)

logger.info("🚀 Streaming started. Press Ctrl+C to stop.")
query.awaitTermination()