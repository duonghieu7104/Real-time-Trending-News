"""
Spark Streaming Processor for ONNX Embedding Generation
Gets data from raw_news topic, processes with ONNX model, sends to processed_data topic
"""

import json
import logging
import os
import numpy as np
from typing import List, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, TimestampType
from kafka import KafkaProducer

# ONNX Runtime imports
import onnxruntime as ort
from transformers import AutoTokenizer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ONNXEmbeddingProcessor:
    """ONNX embedding processor for Spark streaming"""
    
    def __init__(self, model_path: str):
        self.model_path = model_path
        self.session = None
        self.tokenizer = None
        self._initialize_model()
    
    def _initialize_model(self):
        """Initialize ONNX model and tokenizer"""
        try:
            # Load ONNX model
            self.session = ort.InferenceSession(
                os.path.join(self.model_path, "model.onnx"),
                providers=['CPUExecutionProvider']
            )
            
            # Load tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
            
            logger.info("✅ ONNX model and tokenizer loaded successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize model: {e}")
            raise e
    
    def preprocess_text(self, text: str) -> str:
        """Preprocess text for embedding generation"""
        if not text or not isinstance(text, str):
            return ""
        
        # Basic text cleaning
        text = text.strip()
        text = " ".join(text.split())
        
        # Truncate if too long
        if len(text) > 8000:
            text = text[:8000]
        
        return text
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a batch of texts"""
        if not texts:
            return []
        
        try:
            # Preprocess texts
            processed_texts = [self.preprocess_text(text) for text in texts]
            
            # Tokenize texts
            inputs = self.tokenizer(
                processed_texts,
                padding=True,
                truncation=True,
                max_length=512,
                return_tensors="np"
            )
            
            # Run inference
            input_ids = inputs["input_ids"].astype(np.int64)
            attention_mask = inputs["attention_mask"].astype(np.int64)
            
            outputs = self.session.run(
                None,
                {
                    "input_ids": input_ids,
                    "attention_mask": attention_mask
                }
            )
            
            # Extract embeddings (last hidden state)
            embeddings = outputs[0]  # Shape: (batch_size, seq_len, hidden_size)
            
            # Pool embeddings (mean pooling)
            attention_mask_expanded = attention_mask[:, :, None]
            pooled_embeddings = np.sum(embeddings * attention_mask_expanded, axis=1) / np.sum(attention_mask_expanded, axis=1)
            
            # Convert to list of lists
            embeddings_list = pooled_embeddings.tolist()
            
            return embeddings_list
            
        except Exception as e:
            logger.error(f"❌ Error generating embeddings: {e}")
            # Return zero embeddings for failed cases
            return [[0.0] * 1024 for _ in texts]
    
    def process_news_batch(self, news_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of news data and generate embeddings"""
        if not news_data:
            return []
        
        try:
            # Extract texts for embedding
            texts = []
            for item in news_data:
                # Combine title and content for better embeddings
                title = item.get('title', '')
                content = item.get('content', '') or item.get('description', '')
                combined_text = f"{title} {content}".strip()
                texts.append(combined_text)
            
            # Generate embeddings
            embeddings = self.generate_embeddings(texts)
            
            # Add embeddings to news data
            processed_data = []
            for i, item in enumerate(news_data):
                item_copy = item.copy()
                item_copy['embedding'] = embeddings[i] if i < len(embeddings) else [0.0] * 1024
                item_copy['embedding_generated_at'] = datetime.now().isoformat()
                item_copy['embedding_model'] = 'xlm-roberta-onnx'
                processed_data.append(item_copy)
            
            return processed_data
            
        except Exception as e:
            logger.error(f"❌ Error processing news batch: {e}")
            return news_data  # Return original data if processing fails


def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("ONNXEmbeddingProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def create_kafka_producer():
    """Create Kafka producer for sending to processed_data topic"""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=['kafka-v4:29092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=500,
            batch_size=32768,
            compression_type="gzip",
            request_timeout_ms=10000,
            retries=3
        )
        logger.info("✅ Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"❌ Failed to create Kafka producer: {e}")
        return None


def process_batch_with_embeddings(batch_df, batch_id):
    """Process each batch of data with embeddings and send to processed_data topic"""
    try:
        # Convert to list of dictionaries
        news_data = batch_df.toPandas().to_dict('records')
        
        if news_data:
            logger.info(f"📥 Processing batch {batch_id} with {len(news_data)} articles")
            
            # Initialize processor
            processor = ONNXEmbeddingProcessor("/app/model/onnx")
            
            # Process with embeddings
            processed_data = processor.process_news_batch(news_data)
            
            # Send to processed_data topic
            if processed_data:
                producer = create_kafka_producer()
                if producer:
                    for item in processed_data:
                        try:
                            producer.send('processed_data', value=item)
                            logger.info(f"📤 Sent to processed_data topic: {item.get('title', 'No title')}")
                        except Exception as e:
                            logger.error(f"❌ Error sending to Kafka: {e}")
                    
                    producer.flush()
                    producer.close()
                    logger.info(f"✅ Sent {len(processed_data)} processed articles to processed_data topic")
                else:
                    logger.error("❌ Cannot send to Kafka - producer not available")
            
            logger.info(f"✅ Completed processing batch {batch_id}")
            
    except Exception as e:
        logger.error(f"❌ Error processing batch {batch_id}: {e}")


def main():
    """Main function to run the Spark streaming processor"""
    logger.info("🚀 Starting Spark Streaming ONNX Embedding Processor...")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Define schema for Kafka messages
        schema = StructType([
            StructField("title", StringType(), True),
            StructField("content", StringType(), True),
            StructField("description", StringType(), True),
            StructField("url", StringType(), True),
            StructField("source", StringType(), True),
            StructField("published_at", StringType(), True),
            StructField("collected_at", StringType(), True),
            StructField("category", StringType(), True)
        ])
        
        # Create streaming DataFrame from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka-v4:29092") \
            .option("subscribe", "raw_news") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON messages
        df_parsed = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Apply processing to each batch
        query = df_parsed.writeStream \
            .foreachBatch(process_batch_with_embeddings) \
            .option("checkpointLocation", "/tmp/spark-checkpoint") \
            .start()
        
        # Wait for termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("🛑 Stopping processor...")
    except Exception as e:
        logger.error(f"❌ Processor error: {e}")
    finally:
        logger.info("🔌 Processor stopped")


if __name__ == "__main__":
    main()
