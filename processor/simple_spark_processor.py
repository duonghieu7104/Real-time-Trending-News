#!/usr/bin/env python3
"""
Simple Spark processor without tokenizer - just for testing
"""

import json
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, TimestampType
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder.appName("SimpleEmbeddingProcessor") \
        .config("spark.jars", "/opt/spark/work-dir/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/jars/kafka-clients-3.5.0.jar,/opt/spark/work-dir/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/jars/commons-pool2-2.11.1.jar")\
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def create_kafka_producer():
    """Create Kafka producer for sending to processed_data topic"""
    try:
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

def process_batch_simple(batch_df, batch_id):
    """Process each batch of data with simple processing (no embedding)"""
    try:
        logger.info(f"📥 Processing batch {batch_id} with {batch_df.count()} articles")
        
        producer = create_kafka_producer()
        if producer is None:
            logger.error("❌ Cannot create Kafka producer")
            return

        # Process each row
        for row in batch_df.toLocalIterator():
            try:
                # Create simple processed data (without embedding for now)
                processed_data = {
                    "title": row.title,
                    "content": row.content or row.description or "",
                    "url": row.url,
                    "source": row.source,
                    "category": row.category,
                    "published_at": row.published_at,
                    "collected_at": row.collected_at,
                    "processed_at": datetime.now().isoformat(),
                    "embedding": [0.0] * 1024,  # Dummy embedding
                    "embedding_model": "dummy-model"
                }
                
                # Send to processed_data topic
                producer.send('processed_data', value=processed_data)
                logger.info(f"✅ Sent processed article: {row.title}")
                
            except Exception as e:
                logger.error(f"❌ Error processing row: {e}")

        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

        logger.info(f"✅ Completed batch {batch_id}")

    except Exception as e:
        logger.error(f"❌ Error processing batch {batch_id}: {e}")

def main():
    """Main function to run the simple Spark processor"""
    logger.info("🚀 Starting Simple Spark Processor")
    
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
            .option("maxOffsetsPerTrigger", "10") \
            .load()
        
        # Parse JSON messages
        df_parsed = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Apply processing to each batch
        query = df_parsed.writeStream \
            .foreachBatch(process_batch_simple) \
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
