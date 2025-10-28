from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_json, struct, 
    lit, desc, pandas_udf, expr
)
from pyspark.sql.types import *
import numpy as np
from datetime import datetime
import json
import os
import pickle
from bertopic import BERTopic
from transformers import pipeline
import torch
import pandas as pd
import shutil
from concurrent.futures import ThreadPoolExecutor
import gc

print("="*80)
print("🚀 BERTopic + Sentiment Processor (OPTIMIZED v2 - FIXED)")
print("="*80)

# ==================== CẤU HÌNH TỐI ƯU ====================
KAFKA_BOOTSTRAP_SERVERS = "kafka-v4:29092"
KAFKA_INPUT_TOPIC = "processed_data"
KAFKA_OUTPUT_TOPIC = "enriched_news"
MODEL_PATH = "/opt/spark/work-dir/models/bertopic_model.pkl"
CHECKPOINT_PATH = "/opt/spark/work-dir/checkpoints/topic_sentiment"

# CẤU HÌNH TỐI ƯU HÓA
NUM_TOPICS = 20
MIN_TOPIC_SIZE = 5
BATCH_SIZE = 25  # GIẢM TỪ 30 XUỐNG 25
TRIGGER_INTERVAL = "180 seconds"  # TĂNG TỪ 120s LÊN 180s
SENTIMENT_CHUNK_SIZE = 8
MAX_TEXT_LENGTH = 256
MAX_TOPIC_RECORDS = 25  # GIẢM TỪ 30 XUỐNG 25

RESET_CHECKPOINT = os.getenv("RESET_CHECKPOINT", "false").lower() == "true"

# Reset checkpoint nếu cần
if RESET_CHECKPOINT and os.path.exists(CHECKPOINT_PATH):
    print(f"🔄 Đang reset checkpoint: {CHECKPOINT_PATH}")
    shutil.rmtree(CHECKPOINT_PATH)
    print("✅ Checkpoint đã reset")

os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

# ==================== KHỞI TẠO SPARK TỐI ƯU (ĐÃ SỬA LỖI) ====================
print("\n📦 Khởi tạo Spark Session...")

spark = SparkSession.builder \
    .appName("BERTopicSentimentOptimizedV2Fixed") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"  # ✅ THAY ĐỔI: 3.5.0 → 3.4.1
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
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "3000") \
    .config("spark.default.parallelism", "4") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.cleaner.periodicGC.interval", "5min") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.executor.memoryOverhead", "1g") \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"✅ Spark đã khởi tạo")
print(f"📁 Model: {MODEL_PATH}")
print(f"📁 Checkpoint: {CHECKPOINT_PATH}")
print(f"🔗 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"📦 Batch size: {BATCH_SIZE}")
print(f"⏱️  Trigger interval: {TRIGGER_INTERVAL}")
print()

# ==================== LOAD MODELS ====================

print("📦 Đang tải Sentiment Model...")
sentiment_analyzer = None
try:
    sentiment_analyzer = pipeline(
        "sentiment-analysis",
        model="wonrax/phobert-base-vietnamese-sentiment",
        device=0 if torch.cuda.is_available() else -1,
        batch_size=SENTIMENT_CHUNK_SIZE
    )
    print(f"✅ Sentiment model đã tải (device: {'GPU' if torch.cuda.is_available() else 'CPU'})")
except Exception as e:
    print(f"⚠️  Lỗi khi tải sentiment model: {e}")
    sentiment_analyzer = None

# Load BERTopic
bertopic_model = None
topic_keywords_global = {}

if os.path.exists(MODEL_PATH):
    print(f"📂 Đang tải BERTopic model...")
    try:
        with open(MODEL_PATH, 'rb') as f:
            bertopic_model = pickle.load(f)
        
        for topic_id in bertopic_model.get_topics().keys():
            if topic_id >= 0:
                topic_info = bertopic_model.get_topic(topic_id)
                if topic_info:
                    topic_keywords_global[topic_id] = [word for word, _ in topic_info[:5]]
                else:
                    topic_keywords_global[topic_id] = []
            else:
                topic_keywords_global[topic_id] = ["outlier"]
        
        print(f"✅ BERTopic đã tải ({len([t for t in topic_keywords_global.keys() if t >= 0])} topics)")
    except Exception as e:
        print(f"⚠️  Lỗi khi tải BERTopic: {e}")
        bertopic_model = None
else:
    print("⚠️  Không tìm thấy BERTopic model. Topic modeling bị tắt.")

print()

# ==================== PANDAS UDF TỐI ƯU ====================

@pandas_udf(StringType())
def analyze_sentiment_batch(texts: pd.Series) -> pd.Series:
    """Phân tích cảm xúc tối ưu với batch nhỏ"""
    if sentiment_analyzer is None:
        return pd.Series(["neutral"] * len(texts))
    
    results = []
    
    try:
        # Chuẩn bị text (đã được cắt ngắn từ bên ngoài)
        batch_texts = []
        for text in texts:
            if not text or pd.isna(text) or text.strip() == "":
                batch_texts.append("")
            else:
                batch_texts.append(str(text)[:MAX_TEXT_LENGTH])
        
        # Xử lý theo chunk nhỏ
        for i in range(0, len(batch_texts), SENTIMENT_CHUNK_SIZE):
            chunk = batch_texts[i:i+SENTIMENT_CHUNK_SIZE]
            valid_texts = [t for t in chunk if t]
            
            if not valid_texts:
                results.extend(["neutral"] * len(chunk))
                continue
            
            # Inference
            chunk_results = sentiment_analyzer(
                valid_texts, 
                truncation=True, 
                max_length=MAX_TEXT_LENGTH
            )
            
            # Map kết quả
            result_idx = 0
            for orig_text in chunk:
                if not orig_text:
                    results.append("neutral")
                else:
                    res = chunk_results[result_idx]
                    label = res['label'].lower()
                    score = res['score']
                    
                    if 'pos' in label and score > 0.6:
                        results.append("positive")
                    elif 'neg' in label and score > 0.6:
                        results.append("negative")
                    else:
                        results.append("neutral")
                    
                    result_idx += 1
                    
    except Exception as e:
        print(f"⚠️  Lỗi sentiment batch: {e}")
        results = ["neutral"] * len(texts)
    
    return pd.Series(results)

# ==================== TOPIC INFERENCE ====================

def infer_topics_batch(embeddings_array, documents_list):
    """Batch topic inference với error handling"""
    if bertopic_model is None:
        return (
            [-1] * len(documents_list),
            [0.0] * len(documents_list),
            [["no_model"]] * len(documents_list)
        )
    
    try:
        topics, probs = bertopic_model.transform(documents_list, embeddings_array)
        
        topic_scores = []
        for i, (t, p) in enumerate(zip(topics, probs)):
            if t >= 0 and t < len(p):
                topic_scores.append(float(p[t]))
            else:
                topic_scores.append(0.0)
        
        keywords_list = []
        for t in topics:
            keywords_list.append(topic_keywords_global.get(t, ["unknown"]))
        
        return topics, topic_scores, keywords_list
        
    except Exception as e:
        print(f"⚠️  Lỗi topic inference: {e}")
        return (
            [-1] * len(documents_list),
            [0.0] * len(documents_list),
            [["error"]] * len(documents_list)
        )

# ==================== SCHEMA ====================

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

# ==================== KAFKA STREAM (ĐÃ TỐI ƯU) ====================

print(f"📨 Đang kết nối tới Kafka: {KAFKA_INPUT_TOPIC}")

try:
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", BATCH_SIZE) \
        .option("failOnDataLoss", "false") \
        .option("kafkaConsumer.pollTimeoutMs", "180000") \
        .load()
    
    print("✅ Kafka stream đã kết nối")
except Exception as e:
    print(f"❌ Kết nối Kafka thất bại: {e}")
    raise

# Parse JSON
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), input_schema).alias("data")
).select("data.*")

df_with_time = df_parsed.withColumn("processing_time", current_timestamp())

# ==================== BATCH PROCESSING TỐI ƯU ====================

batch_counter = {"count": 0}

def process_batch(batch_df, batch_id):
    """Xử lý batch đã tối ưu hoàn toàn"""
    
    batch_counter["count"] += 1
    
    print(f"\n{'='*80}")
    print(f"📦 Batch #{batch_counter['count']} (ID: {batch_id}) - {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*80}")
    
    try:
        # PERSIST + COUNT 1 LẦN
        batch_df.persist()
        batch_count = batch_df.count()
        
        if batch_count == 0:
            print("⚠️  Batch rỗng - bỏ qua")
            batch_df.unpersist()
            return
        
        print(f"📊 Số bản ghi: {batch_count}")
        
        # Filter với persist
        df_valid = batch_df.filter(
            col("embedding").isNotNull() & 
            (col("content").isNotNull() | col("title").isNotNull())
        ).persist()
        
        valid_count = df_valid.count()
        
        # Giải phóng batch_df ngay
        batch_df.unpersist()
        
        if valid_count == 0:
            print("⚠️  Không có bản ghi hợp lệ")
            df_valid.unpersist()
            return
        
        print(f"✅ Hợp lệ: {valid_count}/{batch_count}")
        
        # ========== TOPIC MODELING (GIỚI HẠN) ==========
        if bertopic_model is not None:
            print("🎯 Đang phân tích topic...")
            
            # CHỈ LẤY TỐI ĐA MAX_TOPIC_RECORDS
            pdf = df_valid.select("_id", "content", "title", "embedding") \
                .limit(MAX_TOPIC_RECORDS) \
                .toPandas()
            
            embeddings = np.array(pdf['embedding'].tolist())
            # RÚT NGẮN TEXT
            documents = pdf['content'].fillna(pdf['title']).str[:500].tolist()
            
            topics, scores, keywords = infer_topics_batch(embeddings, documents)
            
            pdf['topic_id'] = topics
            pdf['topic_score'] = scores
            pdf['topic_keywords'] = keywords
            
            # Back to Spark
            topic_schema = StructType([
                StructField("_id", StringType(), True),
                StructField("topic_id", IntegerType(), True),
                StructField("topic_score", DoubleType(), True),
                StructField("topic_keywords", ArrayType(StringType()), True)
            ])
            
            df_topics = spark.createDataFrame(
                pdf[['_id', 'topic_id', 'topic_score', 'topic_keywords']],
                schema=topic_schema
            )
            
            df_with_topic = df_valid.join(df_topics, on="_id", how="left")
            
            # XÓA BIẾN ĐỂ GIẢI PHÓNG BỘ NHỚ
            del pdf, embeddings, documents, df_topics
            gc.collect()
            
            print("   ✓ Hoàn thành")
        else:
            df_with_topic = df_valid \
                .withColumn("topic_id", lit(-1)) \
                .withColumn("topic_score", lit(0.0)) \
                .withColumn("topic_keywords", expr("array('no_model')"))
        
        # Giải phóng df_valid
        df_valid.unpersist()
        
        # ========== SENTIMENT (RÚT NGẮN TEXT) ==========
        print("😊 Đang phân tích cảm xúc...")
        df_with_sentiment = df_with_topic.withColumn(
            "sentiment",
            analyze_sentiment_batch(expr(f"substring(content, 1, {MAX_TEXT_LENGTH})"))
        )
        print("   ✓ Hoàn thành")
        
        # ========== CHUẨN BỊ OUTPUT ==========
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
        ).persist()
        
        output_count = df_enriched.count()
        print(f"📤 Số output: {output_count}")
        
        # ========== GHI SONG SONG 3 SINK ==========
        def write_kafka():
            try:
                print("   → Kafka...", end=" ", flush=True)
                
                df_kafka = df_enriched.selectExpr(
                    "CAST(doc_id AS STRING) AS key",
                    "to_json(struct(*)) AS value"
                )
                
                df_kafka.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                    .option("topic", KAFKA_OUTPUT_TOPIC) \
                    .option("kafka.acks", "1") \
                    .option("kafka.retries", "3") \
                    .save()
                
                print("✓")
            except Exception as e:
                print(f"✗ ({str(e)[:50]})")
        
        def write_elasticsearch():
            try:
                print("   → Elasticsearch...", end=" ", flush=True)
                
                df_enriched.withColumn("@timestamp", col("processing_time")) \
                    .write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", "elasticsearch-v4") \
                    .option("es.port", "9200") \
                    .option("es.resource", "news_enriched/_doc") \
                    .option("es.mapping.id", "doc_id") \
                    .option("es.batch.size.entries", "500") \
                    .option("es.write.operation", "index") \
                    .mode("append") \
                    .save()
                
                print("✓")
            except Exception as e:
                print(f"✗ ({str(e)[:50]})")
        
        def write_mongodb():
            try:
                print("   → MongoDB...", end=" ", flush=True)
                
                df_enriched.select(
                    col("doc_id").alias("_id"),
                    col("doc_id"),
                    col("topic_id"),
                    col("topic_score").alias("score"),
                    col("sentiment"),
                    lit(datetime.now().strftime("%Y-%m-%d")).alias("model_version"),
                    col("processing_time")
                ).write \
                    .format("mongo") \
                    .mode("append") \
                    .save()
                
                print("✓")
            except Exception as e:
                print(f"✗ ({str(e)[:50]})")
        
        # GHI SONG SONG
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(write_kafka),
                executor.submit(write_elasticsearch),
                executor.submit(write_mongodb)
            ]
            # Đợi tất cả hoàn thành
            for future in futures:
                future.result()
        
        # ========== THỐNG KÊ ==========
        print(f"\n📊 Thống kê:")
        
        if bertopic_model:
            try:
                top_topics = df_enriched.groupBy("topic_id").count() \
                    .orderBy(desc("count")).limit(3).collect()
                
                print(f"   Topics:")
                for row in top_topics:
                    tid = row['topic_id']
                    if tid >= 0 and tid in topic_keywords_global:
                        kw = ', '.join(topic_keywords_global[tid][:3])
                        print(f"      {tid}: {row['count']} docs ({kw})")
            except:
                pass
        
        try:
            sentiments = df_enriched.groupBy("sentiment").count().collect()
            print(f"   Cảm xúc: ", end="")
            sentiment_map = {"positive": "😊", "negative": "😔", "neutral": "😐"}
            print(" | ".join([
                f"{sentiment_map.get(r['sentiment'], '')} {r['sentiment']}: {r['count']}" 
                for r in sentiments
            ]))
        except:
            pass
        
        # GIẢI PHÓNG CACHE
        df_enriched.unpersist()
        gc.collect()
        
        print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"\n❌ Lỗi xử lý batch: {e}")
        import traceback
        traceback.print_exc()
        
        # Cleanup trong trường hợp lỗi
        try:
            batch_df.unpersist()
        except:
            pass
        gc.collect()

# ==================== START STREAMING ====================

print("\n" + "="*80)
print("🚀 BẮT ĐẦU STREAMING")
print("="*80)

query = df_with_time \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .start()

print("\n✅ STREAMING ĐANG HOẠT ĐỘNG")
print(f"   📨 Input: {KAFKA_INPUT_TOPIC}")
print(f"   📤 Output: {KAFKA_OUTPUT_TOPIC}, Elasticsearch, MongoDB")
print(f"   ⏱️  Interval: {TRIGGER_INTERVAL} | Batch: {BATCH_SIZE}")
print(f"   🎯 BERTopic: {'BẬT' if bertopic_model else 'TẮT'} (tối đa {MAX_TOPIC_RECORDS} records)")
print(f"   😊 Sentiment: {'BẬT' if sentiment_analyzer else 'TẮT'} (tối đa {MAX_TEXT_LENGTH} ký tự)")
print(f"   🧵 Ghi song song: 3 sinks (Kafka + ES + Mongo)")
print(f"\n💡 Tối ưu hóa:")
print(f"   • Giảm batch size: {BATCH_SIZE}")
print(f"   • Tăng trigger interval: {TRIGGER_INTERVAL}")
print(f"   • Giới hạn độ dài text: {MAX_TEXT_LENGTH} ký tự")
print(f"   • Ghi song song 3 sink")
print(f"   • Quản lý bộ nhớ với gc.collect()")
print(f"   • ✅ SỬA LỖI: Kafka connector 3.4.1 (tương thích)")
print(f"\n💡 Ctrl+C để dừng | RESET_CHECKPOINT=true để reset offsets\n")
print("="*80 + "\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n🛑 ĐANG DỪNG...")
    query.stop()
    spark.stop()
    print("✅ ĐÃ DỪNG\n")