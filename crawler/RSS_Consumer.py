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
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",   # đọc từ đầu nếu chưa có offset
    enable_auto_commit=True,
    group_id="news-consumer-group",  
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=30000,  # 30 seconds timeout
    request_timeout_ms=15000,   # 15 seconds request timeout
    session_timeout_ms=10000    # 10 seconds session timeout
)

logging.info("🚀 Bắt đầu consume từ Kafka...")
logging.info(f"📡 Listening to topic: {TOPIC}")
logging.info(f"🗄️ MongoDB: {MONGO_URI}/{DB_NAME}/{COLLECTION_NAME}")

try:
    logging.info("🔄 Starting message consumption loop...")
    message_count = 0
    
    for message in consumer:
        try:
            data = message.value  # dict/json từ Producer
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

except KeyboardInterrupt:
    logging.info("🛑 Stopping consumer...")
except Exception as e:
    logging.error(f"❌ Consumer error: {e}")
finally:
    consumer.close()
    mongo_client.close()
    logging.info("🔌 Connections closed")
