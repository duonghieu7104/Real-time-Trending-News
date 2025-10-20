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

print("="*80)
print("🚀 BERTopic + Sentiment Processor for Docker")
print("="*80)

# Khởi tạo Spark Session với config phù hợp cho Docker
spark = SparkSession.builder \
    .appName("BERTopicSentimentProcessor") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.8.0") \
    .config("spark.mongodb.output.uri", "mongodb://mongo-v4:27017/news_db.doc_topics") \
    .config("spark.es.nodes", "elasticsearch-v4") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "news_enriched") \
    .config("spark.es.nodes.wan.only", "false") \
    .config("spark.broadcast.compress", "true") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Cấu hình paths và topics
KAFKA_BOOTSTRAP_SERVERS = "kafka-v4:29092"
KAFKA_INPUT_TOPIC = "processed_data"
KAFKA_OUTPUT_TOPIC = "enriched_news"
MODEL_PATH = "/opt/spark/work-dir/models/bertopic_model.pkl"
CHECKPOINT_PATH = "/opt/spark/work-dir/checkpoints/topic_sentiment"
NUM_TOPICS = 20
MIN_TOPIC_SIZE = 5

print(f"📁 Model path: {MODEL_PATH}")
print(f"📁 Checkpoint path: {CHECKPOINT_PATH}")
print(f"🔗 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"🔗 MongoDB: mongo-v4:27017")
print(f"🔗 Elasticsearch: elasticsearch-v4:9200")
print()

# Tạo thư mục nếu chưa có
os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

# Load Sentiment Analysis Model (PhoBERT cho tiếng Việt)
print("📦 Loading Sentiment Analysis Model...")
try:
    sentiment_analyzer = pipeline(
        "sentiment-analysis",
        model="wonrax/phobert-base-vietnamese-sentiment",
        device=0 if torch.cuda.is_available() else -1
    )
    print("✅ Loaded Vietnamese Sentiment Model (PhoBERT)")
except Exception as e:
    print(f"⚠️  Error loading sentiment model: {e}")
    print("⚠️  Sentiment analysis will return 'neutral' for all documents")
    sentiment_analyzer = None

print()

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
        print(f"⚠️  Sentiment error: {e}")
        return "neutral"

sentiment_udf = udf(analyze_sentiment, StringType())

# Hàm train hoặc load BERTopic model
def get_or_train_bertopic_model(embeddings, documents):
    """Train mới hoặc load BERTopic model"""
    
    if os.path.exists(MODEL_PATH):
        print(f"📂 Loading existing BERTopic model from {MODEL_PATH}")
        try:
            with open(MODEL_PATH, 'rb') as f:
                topic_model = pickle.load(f)
            
            # Kiểm tra tính tương thích của model
            test_topic, test_prob = topic_model.transform([documents[0]], embeddings[:1])
            print(f"✅ Model validation: OK (test topic: {test_topic[0]})")
            return topic_model
        except Exception as e:
            print(f"⚠️  Error loading model: {e}")
            print(f"🔄 Will retrain model...")
            # Xóa model cũ và train lại
            try:
                os.remove(MODEL_PATH)
            except:
                pass
    
    print("🎓 No existing model found. Training new BERTopic model...")
    
    if len(embeddings) < MIN_TOPIC_SIZE:
        print(f"⚠️  Not enough data for training (need at least {MIN_TOPIC_SIZE} documents)")
        return None
    
    try:
        # UMAP để giảm chiều
        umap_model = UMAP(
            n_neighbors=min(15, len(embeddings) - 1),
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
        
        # Khởi tạo BERTopic
        topic_model = BERTopic(
            embedding_model=None,
            umap_model=umap_model,
            hdbscan_model=hdbscan_model,
            nr_topics=NUM_TOPICS,
            top_n_words=5,
            language='vietnamese',
            calculate_probabilities=True,
            verbose=True
        )
        
        # Train model
        print(f"🎯 Training BERTopic with {len(documents)} documents...")
        topics, probs = topic_model.fit_transform(documents, embeddings)
        
        # Lưu model
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        with open(MODEL_PATH, 'wb') as f:
            pickle.dump(topic_model, f)
        
        print(f"✅ Model trained and saved to {MODEL_PATH}")
        print(f"📊 Number of topics: {len(set(topics)) - 1}")  # Trừ topic -1 (outliers)
        
        return topic_model
        
    except Exception as e:
        print(f"❌ Error training BERTopic: {e}")
        import traceback
        traceback.print_exc()
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
print(f"📨 Reading from Kafka topic: {KAFKA_INPUT_TOPIC}")

try:
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 100) \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✅ Kafka stream initialized")
except Exception as e:
    print(f"❌ Error connecting to Kafka: {e}")
    raise

# Parse JSON từ Kafka
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), input_schema).alias("data")
).select("data.*")

# Thêm timestamp xử lý
df_with_time = df_parsed.withColumn("processing_time", current_timestamp())

# Hàm xử lý từng micro-batch
def process_batch(batch_df, batch_id):
    """Xử lý mỗi micro-batch: BERTopic + sentiment + save"""
    
    print(f"\n{'='*80}")
    print(f"📦 Processing Batch #{batch_id} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    try:
        batch_count = batch_df.count()
        if batch_count == 0:
            print("⚠️  Empty batch, skipping...")
            return
        
        print(f"📊 Batch size: {batch_count} records")
        
        # Chuyển sang Pandas để xử lý với BERTopic
        batch_pdf = batch_df.toPandas()
        
        # Lọc documents có embedding hợp lệ
        batch_pdf = batch_pdf[batch_pdf['embedding'].notna()]
        
        if len(batch_pdf) == 0:
            print("⚠️  No valid embeddings in batch, skipping...")
            return
        
        print(f"✅ Processing {len(batch_pdf)} documents with valid embeddings")
        
        # Chuẩn bị embeddings và documents
        embeddings = np.array(batch_pdf['embedding'].tolist())
        documents = batch_pdf['content'].fillna(batch_pdf['title']).tolist()
        
        # Validate embeddings
        print(f"   📏 Embeddings shape: {embeddings.shape}")
        print(f"   📄 Documents count: {len(documents)}")
        
        if embeddings.shape[0] != len(documents):
            print("❌ Mismatch between embeddings and documents count, skipping...")
            return
        
        # 1. Load hoặc train BERTopic model
        topic_model = get_or_train_bertopic_model(embeddings, documents)
        
        if topic_model is None:
            print("❌ Model not available, skipping batch")
            return
        
        # 2. Topic inference
        try:
            print(f"🎯 Running topic inference...")
            
            # Transform với error handling
            topics, probs = topic_model.transform(documents, embeddings)
            
            # Xử lý probabilities an toàn
            topic_scores = []
            for i, (t, p) in enumerate(zip(topics, probs)):
                if t >= 0 and t < len(p):
                    topic_scores.append(float(p[t]))
                else:
                    topic_scores.append(0.0)
            
            batch_pdf['topic_id'] = topics
            batch_pdf['topic_score'] = topic_scores
            
            # 3. Extract topic keywords
            topic_keywords_dict = {}
            for topic_id in set(topics):
                if topic_id >= 0:
                    topic_info = topic_model.get_topic(topic_id)
                    if topic_info:
                        keywords = [word for word, _ in topic_info[:5]]
                        topic_keywords_dict[topic_id] = keywords
                    else:
                        topic_keywords_dict[topic_id] = []
                else:
                    topic_keywords_dict[topic_id] = ["outlier"]
            
            batch_pdf['topic_keywords'] = batch_pdf['topic_id'].map(topic_keywords_dict)
            
            print(f"✅ Topic modeling completed")
            
        except Exception as e:
            print(f"❌ Error in topic inference: {e}")
            import traceback
            traceback.print_exc()
            return
        
        # 4. Chuyển lại sang Spark DataFrame
        batch_pdf_clean = batch_pdf.drop(columns=['embedding', 'embedding_model', 'embedding_generated_at'])
        
        # Convert topic_keywords từ list sang string (JSON)
        batch_pdf_clean['topic_keywords'] = batch_pdf_clean['topic_keywords'].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else "[]"
        )
        
        # Define schema
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
            StructField("topic_keywords", StringType(), True)
        ])
        
        df_with_topic = spark.createDataFrame(batch_pdf_clean, schema=schema)
        
        # Parse topic_keywords thành array
        df_with_topic = df_with_topic.withColumn(
            "topic_keywords",
            from_json(col("topic_keywords"), ArrayType(StringType()))
        )
        
        # 5. Sentiment Analysis
        print(f"😊 Running sentiment analysis...")
        df_with_sentiment = df_with_topic.withColumn(
            "sentiment",
            sentiment_udf(col("content"))
        )
        
        # 6. Final DataFrame
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
        
        # 7. Ghi vào Kafka
        try:
            print(f"📤 Writing to Kafka topic: {KAFKA_OUTPUT_TOPIC}")
            df_kafka_output = df_enriched.select(
                to_json(struct("*")).alias("value")
            )
            
            df_kafka_output.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", KAFKA_OUTPUT_TOPIC) \
                .save()
            
            print(f"✅ Sent {df_kafka_output.count()} documents to Kafka")
        except Exception as e:
            print(f"⚠️  Error writing to Kafka: {e}")
        
        # 8. Ghi vào Elasticsearch
        try:
            print(f"📤 Indexing to Elasticsearch...")
            df_es = df_enriched.withColumn("@timestamp", col("processing_time"))
            
            df_es.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "elasticsearch-v4") \
                .option("es.port", "9200") \
                .option("es.resource", "news_enriched/_doc") \
                .option("es.mapping.id", "doc_id") \
                .option("es.nodes.wan.only", "false") \
                .option("es.write.operation", "index") \
                .option("es.batch.size.entries", "1000") \
                .option("es.batch.size.bytes", "10mb") \
                .mode("append") \
                .save()
            
            print(f"✅ Indexed {df_es.count()} documents to Elasticsearch")
            
        except Exception as e:
            print(f"⚠️  Error writing to Elasticsearch: {e}")
            import traceback
            traceback.print_exc()
        
        # 9. Ghi vào MongoDB
        try:
            print(f"📤 Saving to MongoDB...")
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
            
            print(f"✅ Saved {df_mongo.count()} records to MongoDB")
        except Exception as e:
            print(f"⚠️  Error writing to MongoDB: {e}")
            import traceback
            traceback.print_exc()
        
        # 10. In thống kê
        print(f"\n📊 Batch Statistics:")
        print(f"   Total documents: {df_enriched.count()}")
        
        topic_dist = df_enriched.groupBy("topic_id").count().orderBy(desc("count"))
        print(f"\n   🏷️  Topic Distribution (Top 5):")
        for row in topic_dist.limit(5).collect():
            if row['topic_id'] in topic_keywords_dict:
                keywords_list = topic_keywords_dict[row['topic_id']]
                print(f"      Topic {row['topic_id']}: {row['count']} docs - {keywords_list}")
        
        sentiment_dist = df_enriched.groupBy("sentiment").count().orderBy(desc("count"))
        print(f"\n   😊 Sentiment Distribution:")
        for row in sentiment_dist.collect():
            emoji = {"positive": "😊", "negative": "😔", "neutral": "😐"}.get(row['sentiment'], "")
            print(f"      {emoji} {row['sentiment']}: {row['count']} docs")
        
        print(f"\n{'='*80}\n")
        
    except Exception as e:
        print(f"❌ Error processing batch #{batch_id}: {e}")
        import traceback
        traceback.print_exc()

# Chạy streaming query
print("\n" + "="*80)
print("🚀 Starting Streaming Query...")
print("="*80)

query = df_with_time \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="30 seconds") \
    .start()

print("\n✅ BERTopic + Sentiment Processor started!")
print(f"   📨 Input: Kafka topic '{KAFKA_INPUT_TOPIC}'")
print(f"   📤 Output: Kafka '{KAFKA_OUTPUT_TOPIC}', Elasticsearch 'news_enriched', MongoDB 'doc_topics'")
print(f"   ⏱️  Processing interval: 30 seconds")
print(f"   🎯 Model: BERTopic with max {NUM_TOPICS} topics")
print(f"   😊 Sentiment: PhoBERT Vietnamese")
print(f"\n💡 Press Ctrl+C to stop...\n")
print("="*80 + "\n")

# Chờ cho đến khi dừng
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n" + "="*80)
    print("🛑 Stopping streaming query...")
    print("="*80)
    query.stop()
    spark.stop()
    print("✅ Stopped successfully")
    print("="*80)