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

# Optional Vietnamese word segmentation
VN_WORDSEG_ENABLED = os.environ.get("VN_WORDSEG", "0") == "1"
try:
    if VN_WORDSEG_ENABLED:
        from pyvi.ViTokenizer import tokenize as vi_tokenize
    else:
        vi_tokenize = None
except Exception:
    vi_tokenize = None
    VN_WORDSEG_ENABLED = False

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
            
            # Load tokenizer with fallback options
            self.tokenizer = None
            tokenizer_options = [
                "vinai/phobert-base",
                "xlm-roberta-base", 
                self.model_path
            ]
            
            for tokenizer_name in tokenizer_options:
                try:
                    self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
                    logger.info(f"✅ Tokenizer loaded from {tokenizer_name}")
                    break
                except Exception as e:
                    logger.warning(f"⚠️ Failed to load tokenizer from {tokenizer_name}: {e}")
                    continue
            
            if self.tokenizer is None:
                raise Exception("Failed to load any tokenizer")
            
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
        
        # Optional Vietnamese word segmentation (tokenize with spaces)
        if VN_WORDSEG_ENABLED and vi_tokenize is not None:
            try:
                text = vi_tokenize(text)
            except Exception:
                # If segmentation fails, use the cleaned text
                pass
        
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


def create_spark_session(master_url=None):
    """Create Spark session. If master_url is None, do not override spark-submit master."""
    builder = SparkSession.builder.appName("ONNXEmbeddingProcessor") 
    if master_url:
        builder = builder.master(master_url)
    return builder \
        .config("spark.jars", "/opt/spark/work-dir/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/jars/kafka-clients-3.5.0.jar,/opt/spark/work-dir/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/jars/commons-pool2-2.11.1.jar")\
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


def _iter_rows_as_dicts(df, chunk_size: int = 20):
    """Yield rows as dictionaries in chunks to control memory usage."""
    buffer = []
    for row in df.toLocalIterator():
        buffer.append(row.asDict(recursive=True))
        if len(buffer) >= chunk_size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer


def process_batch_with_embeddings(batch_df, batch_id):
    """Process each batch of data with embeddings and send to processed_data topic (memory-safe)."""
    try:
        total_processed = 0

        # Initialize once per batch on driver
        processor = ONNXEmbeddingProcessor("/opt/spark/work-dir/model/embedding")
        producer = create_kafka_producer()

        if producer is None:
            logger.error("❌ Cannot create Kafka producer")
            return

        minimal_df = batch_df.select(
            col("title"), col("content"), col("description"), col("url"),
            col("source"), col("published_at"), col("collected_at"), col("category")
        )

        for chunk in _iter_rows_as_dicts(minimal_df, chunk_size=20):
            logger.info(f"📥 Processing batch {batch_id} chunk with {len(chunk)} articles")
            processed_data = processor.process_news_batch(chunk)

            for item in processed_data:
                try:
                    producer.send('processed_data', value=item)
                except Exception as e:
                    logger.error(f"❌ Error sending to Kafka: {e}")

            total_processed += len(processed_data)

        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

        logger.info(f"✅ Completed batch {batch_id} - sent {total_processed} articles")

    except Exception as e:
        logger.error(f"❌ Error processing batch {batch_id}: {e}")


def main():
    """Main function to run the Spark streaming processor"""
    import argparse
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--master", dest="master", default=None)
    # Allow unknown args so spark-submit extras don't break parsing
    args, _ = parser.parse_known_args()
    master_url = args.master
    
    logger.info(f"🚀 Starting Spark Streaming ONNX Embedding Processor with master: {master_url}")
    
    try:
        # Create Spark session (do not override if spark-submit provided master)
        spark = create_spark_session(master_url)
        
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
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", "50") \
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
