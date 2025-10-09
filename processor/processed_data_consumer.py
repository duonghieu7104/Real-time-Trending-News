

import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient, errors

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka-v4:29092"
KAFKA_TOPIC = "processed_data"

# MongoDB configuration
MONGO_URI = "mongodb://mongo-v4:27017"
DB_NAME = "news_db"
COLLECTION_NAME = "processed_articles"

def create_kafka_consumer():
    """Create Kafka consumer with retry mechanism"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logger.info(f"🔄 Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="processed-data-consumer-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                request_timeout_ms=30000,
                session_timeout_ms=15000,
                max_poll_interval_ms=300000
            )
            logger.info("✅ Kafka consumer connected successfully!")
            return consumer
        except Exception as e:
            logger.warning(f"⚠️ Kafka connection failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                import time
                time.sleep(retry_delay)
            else:
                logger.error("❌ Failed to connect to Kafka after all retries")
                raise e
    
    return None

def create_mongodb_connection():
    """Create MongoDB connection"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Create unique index on URL
        collection.create_index("url", unique=True)
        
        logger.info("✅ MongoDB connection established")
        return client, db, collection
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        raise e

def validate_processed_data(data):
    """Validate that the processed data has required fields"""
    required_fields = ['title', 'url', 'embedding']
    for field in required_fields:
        if field not in data or not data[field]:
            return False, f"Missing or empty field: {field}"
    return True, "Valid"

def format_datetime(dt: datetime):
    """Format datetime for storage"""
    return dt.strftime("%d/%m/%Y/%H/%M/%S")

def run_processed_data_consumer():
    """Main consumer function"""
    logger.info("🚀 Starting processed_data consumer...")
    
    # Create connections
    consumer = create_kafka_consumer()
    if not consumer:
        logger.error("❌ Cannot start consumer without Kafka connection")
        return
    
    mongo_client, db, collection = create_mongodb_connection()
    
    logger.info(f"📡 Listening to topic: {KAFKA_TOPIC}")
    logger.info(f"🗄️ MongoDB: {MONGO_URI}/{DB_NAME}/{COLLECTION_NAME}")
    
    try:
        message_count = 0
        
        for message in consumer:
            try:
                data = message.value
                message_count += 1
                
                logger.info(f"📥 Received message #{message_count} from {data.get('source', 'unknown')} (partition={message.partition}, offset={message.offset})")
                logger.info(f"📰 Title: {data.get('title', 'No title')}")
                
                # Validate data structure
                is_valid, validation_msg = validate_processed_data(data)
                if not is_valid:
                    logger.warning(f"⚠️ Invalid data skipped: {validation_msg}")
                    continue
                
                # Add processing timestamp
                data["processed_at"] = format_datetime(datetime.now())
                
                # Save to MongoDB
                try:
                    collection.insert_one(data)
                    logger.info(f"✅ Inserted processed article: {data['title']} from {data.get('source', 'unknown')}")
                except errors.DuplicateKeyError:
                    logger.debug(f"⚠️ Duplicate skipped: {data.get('url')}")
                except Exception as e:
                    logger.error(f"❌ MongoDB error: {e}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("🛑 Stopping consumer...")
    except Exception as e:
        logger.error(f"❌ Consumer error: {e}")
    finally:
        consumer.close()
        mongo_client.close()
        logger.info("🔌 Connections closed")

if __name__ == "__main__":
    run_processed_data_consumer()
