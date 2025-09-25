from kafka import KafkaConsumer
import json
import datetime
import logging
from pymongo import MongoClient, errors

# ========== Logging ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ========== Kafka Config ==========
KAFKA_BROKER = "kafka-v4:29092"
TOPIC = "raw_news"

# ========== MongoDB Config ==========
MONGO_URI = "mongodb://mongo-v4:27017"
DB_NAME = "news_db"
COLLECTION_NAME = "articles"

# Kết nối MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

# Tạo unique index để chống trùng (theo url)
collection.create_index("url", unique=True)

# ========== Helper Functions ==========
def format_datetime(dt: datetime.datetime):
    return dt.strftime("%d/%m/%Y/%H/%M/%S")

def validate_news_data(data):
    """Validate that the news data has required fields"""
    required_fields = ['source', 'url', 'title', 'content']
    for field in required_fields:
        if field not in data or not data[field]:
            return False, f"Missing or empty field: {field}"
    return True, "Valid"

# ========== Kafka Consumer ==========
def create_kafka_consumer():
    """Create Kafka consumer with retry mechanism"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logging.info(f"🔄 Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="news-consumer-group",  
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                consumer_timeout_ms=30000,  # 30 seconds timeout
                request_timeout_ms=15000,   # 15 seconds request timeout
                session_timeout_ms=10000    # 10 seconds session timeout
            )
            logging.info("✅ Kafka consumer connected successfully!")
            return consumer
        except Exception as e:
            logging.warning(f"⚠️ Kafka connection failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                logging.info(f"⏳ Retrying in {retry_delay} seconds...")
                import time
                time.sleep(retry_delay)
            else:
                logging.error("❌ Failed to connect to Kafka after all retries")
                raise e
    
    return None

# ========== Main Consumer Loop ==========
def run_continuous_consumer():
    logging.info("🚀 Starting continuous RSS consumer...")
    logging.info(f"📡 Listening to topic: {TOPIC}")
    logging.info(f"🗄️ MongoDB: {MONGO_URI}/{DB_NAME}/{COLLECTION_NAME}")
    
    consumer = create_kafka_consumer()
    if not consumer:
        logging.error("❌ Cannot start consumer without Kafka connection")
        return
    
    message_count = 0
    
    try:
        while True:
            try:
                # Poll for messages with timeout
                message_batch = consumer.poll(timeout_ms=10000)  # 10 second timeout
                
                if not message_batch:
                    logging.info("⏳ No messages received, waiting...")
                    continue
                
                # Process each message
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            data = message.value
                            message_count += 1
                            
                            logging.info(f"📥 Received message #{message_count} from {data.get('source', 'unknown')} (partition={message.partition}, offset={message.offset})")
                            logging.info(f"📰 Title: {data.get('title', 'No title')}")
                            
                            # Validate data structure
                            is_valid, validation_msg = validate_news_data(data)
                            if not is_valid:
                                logging.warning(f"⚠️ Invalid data skipped: {validation_msg}")
                                continue

                            # Ghi thời gian nhận được (collected_at) - chỉ nếu chưa có
                            if 'collected_at' not in data or not data['collected_at']:
                                data["collected_at"] = format_datetime(datetime.datetime.now())

                            # Lưu vào MongoDB
                            try:
                                collection.insert_one(data)
                                logging.info(f"✅ Inserted into MongoDB: {data['title']} from {data.get('source', 'unknown')}")
                            except errors.DuplicateKeyError:
                                logging.debug(f"⚠️ Duplicate skipped: {data.get('url')}")
                            except Exception as e:
                                logging.error(f"❌ MongoDB error: {e}")
                                
                        except Exception as e:
                            logging.error(f"❌ Error processing message: {e}")
                            continue
                            
            except Exception as e:
                logging.error(f"❌ Error in consumer loop: {e}")
                import time
                time.sleep(5)  # Wait 5 seconds before retrying
                continue

    except KeyboardInterrupt:
        logging.info("🛑 Stopping continuous consumer...")
    except Exception as e:
        logging.error(f"❌ Consumer error: {e}")
    finally:
        consumer.close()
        mongo_client.close()
        logging.info("🔌 Connections closed")

if __name__ == "__main__":
    run_continuous_consumer()
