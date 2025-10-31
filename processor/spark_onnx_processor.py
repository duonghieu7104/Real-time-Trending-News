#!/usr/bin/env python3
"""
Spark job để xử lý ONNX model loading và embedding processing
Chạy trên Spark cluster thay vì Airflow để tối ưu performance
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StringType, ArrayType, FloatType
import onnxruntime as ort
import numpy as np
from transformers import AutoTokenizer
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Tạo Spark session với cấu hình tối ưu cho ML processing"""
    return SparkSession.builder \
        .appName("ONNX_Embedding_Processor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def load_onnx_model(model_path):
    """Load ONNX model một lần và cache"""
    try:
        session = ort.InferenceSession(model_path)
        logger.info(f"✅ Loaded ONNX model from {model_path}")
        return session
    except Exception as e:
        logger.error(f"❌ Failed to load ONNX model: {e}")
        return None

def load_tokenizer(model_name="vinai/phobert-base"):
    """Load tokenizer"""
    try:
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        logger.info(f"✅ Loaded tokenizer: {model_name}")
        return tokenizer
    except Exception as e:
        logger.error(f"❌ Failed to load tokenizer: {e}")
        return None

def process_text_embedding(text, onnx_session, tokenizer):
    """Process text để tạo embedding sử dụng ONNX model"""
    try:
        if not text or not onnx_session or not tokenizer:
            return None
            
        # Tokenize text
        inputs = tokenizer(text, return_tensors="np", max_length=256, 
                          truncation=True, padding=True)
        
        # Run ONNX inference
        input_ids = inputs["input_ids"].astype(np.int64)
        attention_mask = inputs["attention_mask"].astype(np.int64)
        
        outputs = onnx_session.run(
            None, 
            {
                "input_ids": input_ids,
                "attention_mask": attention_mask
            }
        )
        
        # Extract embeddings (last hidden state)
        embeddings = outputs[0][0]  # [batch_size, seq_len, hidden_size]
        
        # Pool embeddings (mean pooling)
        pooled_embeddings = np.mean(embeddings, axis=1)
        
        return pooled_embeddings.tolist()
        
    except Exception as e:
        logger.error(f"❌ Error processing text embedding: {e}")
        return None

def process_batch_embeddings(iterator):
    """Process batch of texts for embeddings"""
    # Load model and tokenizer once per worker
    model_path = "/opt/spark/work-dir/model/embedding/model.onnx"
    onnx_session = load_onnx_model(model_path)
    tokenizer = load_tokenizer()
    
    if not onnx_session or not tokenizer:
        logger.error("❌ Failed to load model or tokenizer")
        return
    
    for row in iterator:
        try:
            text = row.content if hasattr(row, 'content') else str(row)
            embedding = process_text_embedding(text, onnx_session, tokenizer)
            
            if embedding:
                yield {
                    'id': getattr(row, 'id', None),
                    'text': text,
                    'embedding': embedding,
                    'processed_at': getattr(row, 'processed_at', None)
                }
        except Exception as e:
            logger.error(f"❌ Error processing row: {e}")
            continue

def main():
    """Main Spark job function"""
    logger.info("🚀 Starting ONNX Embedding Processing Spark Job")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read data from Kafka hoặc MongoDB
        # Ví dụ: đọc từ MongoDB
        df = spark.read.format("mongo") \
            .option("uri", "mongodb://mongo-v4:27017") \
            .option("database", "news_db") \
            .option("collection", "articles") \
            .load()
        
        logger.info(f"📊 Loaded {df.count()} articles for processing")
        
        # Process embeddings using mapPartitions for efficiency
        embeddings_rdd = df.rdd.mapPartitions(process_batch_embeddings)
        
        # Convert back to DataFrame
        embeddings_df = spark.createDataFrame(embeddings_rdd)
        
        # Save results
        embeddings_df.write \
            .format("mongo") \
            .option("uri", "mongodb://mongo-v4:27017") \
            .option("database", "news_db") \
            .option("collection", "embeddings") \
            .mode("append") \
            .save()
        
        logger.info("✅ Embedding processing completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Spark job failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
