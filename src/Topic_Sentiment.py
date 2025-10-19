from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
from datetime import datetime
import json
import os
import pickle
from bertopic import BERTopic
from sklearn.cluster import KMeans
from umap import UMAP
from hdbscan import HDBSCAN
from transformers import pipeline
import torch
import pandas as pd

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("BERTopicSentimentProcessor") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.8.0") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/news_db.doc_topics") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "news_enriched") \
    .config("spark.broadcast.compress", "true") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Cấu hình
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_INPUT_TOPIC = "processed_data"
KAFKA_OUTPUT_TOPIC = "enriched_news"
MODEL_PATH = "models/bertopic_model.pkl"
CHECKPOINT_PATH = "checkpoints/topic_sentiment"
NUM_TOPICS = 20
MIN_TOPIC_SIZE = 5

# Load Sentiment Analysis Model (PhoBERT cho tiếng Việt)
try:
    sentiment_analyzer = pipeline(
        "sentiment-analysis",
        model="wonrax/phobert-base-vietnamese-sentiment",
        device=0 if torch.cuda.is_available() else -1
    )
    print(" Loaded Vietnamese Sentiment Model (PhoBERT)")
except Exception as e:
    print(f" Error loading sentiment model: {e}")
    sentiment_analyzer = None

# UDF cho Sentiment Analysis
def analyze_sentiment(text):
    """Phân tích cảm xúc văn bản tiếng Việt"""
    if not text or sentiment_analyzer is None:
        return "neutral"
    
    try:
        # Truncate text để tránh quá dài
        text_truncated = text[:512] if len(text) > 512 else text
        result = sentiment_analyzer(text_truncated)[0]
        label = result['label'].lower()
        score = result['score']
        
        # Map labels
        if 'pos' in label or 'positive' in label:
            return "positive" if score > 0.6 else "neutral"
        elif 'neg' in label or 'negative' in label:
            return "negative" if score > 0.6 else "neutral"
        else:
            return "neutral"
    except Exception as e:
        print(f"Sentiment error: {e}")
        return "neutral"

sentiment_udf = udf(analyze_sentiment, StringType())

# Hàm train hoặc load BERTopic model
def get_or_train_bertopic_model(embeddings, documents):
    """Train mới hoặc load BERTopic model"""
    
    if os.path.exists(MODEL_PATH):
        print(f" Loading existing BERTopic model from {MODEL_PATH}")
        try:
            with open(MODEL_PATH, 'rb') as f:
                topic_model = pickle.load(f)
            
            # Kiểm tra tính tương thích của model
            test_topic, test_prob = topic_model.transform([documents[0]], embeddings[:1])
            print(f"   Model validation: OK (test topic: {test_topic[0]})")
            return topic_model
        except Exception as e:
            print(f" Error loading model: {e}")
            print(f"   Will retrain model...")
            # Xóa model cũ và train lại
            os.remove(MODEL_PATH)
    
    print(" No existing model found. Training new BERTopic model...")
    
    if len(embeddings) < MIN_TOPIC_SIZE:
        print(f"Not enough data for training (need at least {MIN_TOPIC_SIZE} documents)")
        return None
    
    try:
        # UMAP để giảm chiều
        umap_model = UMAP(
            n_neighbors=15,
            n_components=5,
            min_dist=0.0,
            metric='cosine',
            random_state=42
        )
        
        # HDBSCAN cho clustering
        hdbscan_model = HDBSCAN(
            min_cluster_size=MIN_TOPIC_SIZE,
            metric='euclidean',
            cluster_selection_method='eom',
            prediction_data=True
        )
        
        # Khởi tạo BERTopic với embedding model = None (vì đã có embeddings)
        topic_model = BERTopic(
            embedding_model=None,  # Quan trọng: không dùng embedding model
            umap_model=umap_model,
            hdbscan_model=hdbscan_model,
            nr_topics=NUM_TOPICS,
            top_n_words=5,
            language='vietnamese',
            calculate_probabilities=True,
            verbose=True
        )
        
        # Train model
        print(f" Training BERTopic with {len(documents)} documents...")
        topics, probs = topic_model.fit_transform(documents, embeddings)
        
        # Lưu model
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        with open(MODEL_PATH, 'wb') as f:
            pickle.dump(topic_model, f)
        
        print(f" Model trained and saved to {MODEL_PATH}")
        print(f"  Number of topics: {len(set(topics)) - 1}")  # Trừ topic -1 (outliers)
        
        return topic_model
        
    except Exception as e:
        print(f" Error training BERTopic: {e}")
        return None

# Schema cho Kafka message
input_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("content", StringType(), True),
    StructField("url", StringType(), True),
    StructField("source", StringType(), True),
    StructField("category", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("collected_at", StringType(), True),
    StructField("processed_at", StringType(), True),
    StructField("embedding", ArrayType(DoubleType()), True),
    StructField("embedding_model", StringType(), True),
    StructField("embedding_generated_at", StringType(), True)
])

# Đọc dữ liệu từ Kafka
print(f" Reading from Kafka topic: {KAFKA_INPUT_TOPIC}")

df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 100) \
    .load()

# Parse JSON từ Kafka
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), input_schema).alias("data")
).select("data.*")

# Thêm timestamp xử lý
df_with_time = df_parsed.withColumn("processing_time", current_timestamp())

# Hàm xử lý từng micro-batch
def process_batch(batch_df, batch_id):
    """Xử lý mỗi micro-batch: BERTopic + sentiment + save"""
    
    print(f"\n{'='*60}")
    print(f" Processing Batch #{batch_id}")
    print(f"{'='*60}")
    
    if batch_df.count() == 0:
        print(" Empty batch, skipping...")
        return
    
    # Chuyển sang Pandas để xử lý với BERTopic
    batch_pdf = batch_df.toPandas()
    
    # Lọc documents có embedding hợp lệ
    batch_pdf = batch_pdf[batch_pdf['embedding'].notna()]
    
    if len(batch_pdf) == 0:
        print(" No valid embeddings in batch, skipping...")
        return
    
    print(f" Processing {len(batch_pdf)} documents with embeddings")
    
    # Chuẩn bị embeddings và documents
    embeddings = np.array(batch_pdf['embedding'].tolist())
    documents = batch_pdf['content'].fillna(batch_pdf['title']).tolist()
    
    # Validate embeddings
    print(f"   Embeddings shape: {embeddings.shape}")
    print(f"   Documents count: {len(documents)}")
    
    if embeddings.shape[0] != len(documents):
        print("Mismatch between embeddings and documents count, skipping...")
        return
    
    # 1. Load hoặc train BERTopic model
    topic_model = get_or_train_bertopic_model(embeddings, documents)
    
    if topic_model is None:
        print(" Model not available, skipping batch")
        return
    
    # 2. Topic inference
    try:
        # Kiểm tra dimension của embeddings
        print(f"   Embedding shape: {embeddings.shape}")
        
        # Transform với error handling
        topics, probs = topic_model.transform(documents, embeddings)
        
        # Xử lý probabilities an toàn
        topic_scores = []
        for i, (t, p) in enumerate(zip(topics, probs)):
            if t >= 0 and t < len(p):
                topic_scores.append(float(p[t]))
            else:
                # Topic -1 (outlier) hoặc invalid
                topic_scores.append(0.0)
        
        batch_pdf['topic_id'] = topics
        batch_pdf['topic_score'] = topic_scores
        
        # 3. Extract topic keywords
        topic_keywords_dict = {}
        for topic_id in set(topics):
            if topic_id >= 0:  # Bỏ qua outliers (topic -1)
                topic_info = topic_model.get_topic(topic_id)
                if topic_info:
                    keywords = [word for word, _ in topic_info[:5]]
                    topic_keywords_dict[topic_id] = keywords
                else:
                    topic_keywords_dict[topic_id] = []
            else:
                topic_keywords_dict[topic_id] = ["outlier"]
        
        batch_pdf['topic_keywords'] = batch_pdf['topic_id'].map(topic_keywords_dict)
        
        print(f" Topic modeling completed")
        
    except Exception as e:
        print(f" Error in topic inference: {e}")
        return
    
    # 4. Chuyển lại sang Spark DataFrame với schema rõ ràng
    # Xóa cột embedding để tránh lỗi type inference
    batch_pdf_clean = batch_pdf.drop(columns=['embedding', 'embedding_model', 'embedding_generated_at'])
    
    # Convert topic_keywords từ list sang string (JSON)
    batch_pdf_clean['topic_keywords'] = batch_pdf_clean['topic_keywords'].apply(
        lambda x: json.dumps(x) if isinstance(x, list) else "[]"
    )
    
    # Define schema rõ ràng
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    schema = StructType([
        StructField("_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("content", StringType(), True),
        StructField("url", StringType(), True),
        StructField("source", StringType(), True),
        StructField("category", StringType(), True),
        StructField("published_at", StringType(), True),
        StructField("collected_at", StringType(), True),
        StructField("processed_at", StringType(), True),
        StructField("processing_time", StringType(), True),
        StructField("topic_id", IntegerType(), True),
        StructField("topic_score", DoubleType(), True),
        StructField("topic_keywords", StringType(), True)  # JSON string
    ])
    
    df_with_topic = spark.createDataFrame(batch_pdf_clean, schema=schema)
    
    # Parse lại topic_keywords thành array
    df_with_topic = df_with_topic.withColumn(
        "topic_keywords",
        from_json(col("topic_keywords"), ArrayType(StringType()))
    )
    
    # 5. Sentiment Analysis
    df_with_sentiment = df_with_topic.withColumn(
        "sentiment",
        sentiment_udf(col("content"))
    )
    
    # 6. Rename _id thành doc_id
    df_enriched = df_with_sentiment.select(
        col("_id").alias("doc_id"),
        col("title"),
        col("content"),
        col("published_at"),
        col("source"),
        col("url"),
        col("category"),
        col("topic_id"),
        col("topic_keywords"),
        col("topic_score"),
        col("sentiment"),
        col("processing_time")
    )
    
    # 7. Ghi vào Kafka (enriched_news)
    df_kafka_output = df_enriched.select(
        to_json(struct("*")).alias("value")
    )
    
    try:
        df_kafka_output.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", KAFKA_OUTPUT_TOPIC) \
            .save()
        print(f" Sent {df_kafka_output.count()} documents to Kafka topic: {KAFKA_OUTPUT_TOPIC}")
    except Exception as e:
        print(f" Error writing to Kafka: {e}")
    
    # 8. Ghi vào Elasticsearch
    try:
        df_es = df_enriched.withColumn("@timestamp", col("processing_time"))
        
        df_es.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "news_enriched") \
            .option("es.mapping.id", "doc_id") \
            .mode("append") \
            .save()
        
        print(f" Indexed {df_es.count()} documents to Elasticsearch: news_enriched")
    except Exception as e:
        print(f" Error writing to Elasticsearch: {e}")
    
    # 9. Ghi vào MongoDB (doc_topics collection)
    try:
        model_version = datetime.now().strftime("%Y-%m-%d")
        
        df_mongo = df_enriched.select(
            col("doc_id").alias("_id"),
            col("doc_id"),
            col("topic_id"),
            col("topic_score").alias("score"),
            col("sentiment"),
            lit(model_version).alias("model_version"),
            col("processing_time")
        )
        
        df_mongo.write \
            .format("mongo") \
            .mode("append") \
            .save()
        
        print(f" Saved {df_mongo.count()} records to MongoDB: doc_topics")
    except Exception as e:
        print(f" Error writing to MongoDB: {e}")
    
    # 10. In thống kê
    print(f"\n Batch Statistics:")
    print(f"   Total documents: {df_enriched.count()}")
    
    topic_dist = df_enriched.groupBy("topic_id").count().orderBy(desc("count"))
    print(f"\n   Topic Distribution (Top 5):")
    for row in topic_dist.limit(5).collect():
        keywords = row['topic_id']
        if row['topic_id'] in topic_keywords_dict:
            keywords_list = topic_keywords_dict[row['topic_id']]
            print(f"     Topic {row['topic_id']}: {row['count']} docs - {keywords_list}")
    
    sentiment_dist = df_enriched.groupBy("sentiment").count().orderBy(desc("count"))
    print(f"\n   Sentiment Distribution:")
    for row in sentiment_dist.collect():
        print(f"     {row['sentiment']}: {row['count']} docs")
    
    print(f"\n{'='*60}\n")

# Chạy streaming query
query = df_with_time \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="30 seconds") \
    .start()

print(" BERTopic + Sentiment Processor started!")
print(f"   Input: Kafka topic '{KAFKA_INPUT_TOPIC}'")
print(f"   Output: Kafka '{KAFKA_OUTPUT_TOPIC}', Elasticsearch 'news_enriched', MongoDB 'doc_topics'")
print(f"   Processing interval: 30 seconds")
print(f"   Model: BERTopic with max {NUM_TOPICS} topics")
print(f"   Sentiment: PhoBERT Vietnamese")
print("\nPress Ctrl+C to stop...\n")

# Chờ cho đến khi dừng
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n Stopping streaming query...")
    query.stop()
    spark.stop()
    print(" Stopped successfully")