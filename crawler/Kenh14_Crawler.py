import feedparser
from kafka import KafkaProducer
import json
from datetime import datetime
import time
import logging
from pymongo import MongoClient, errors
from bs4 import BeautifulSoup

# ================= Logging =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ================= Kafka Config =================
KAFKA_BOOTSTRAP_SERVERS = "kafka-v4:29092"
KAFKA_TOPIC = "raw_news"

def create_kafka_producer():
    """Create Kafka producer with retry mechanism"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logging.info(f"🔄 Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                linger_ms=500,
                batch_size=32768,
                compression_type="gzip",
                request_timeout_ms=10000,
                retries=3
            )
            logging.info("✅ Kafka producer connected successfully!")
            return producer
        except Exception as e:
            logging.warning(f"⚠️ Kafka connection failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                logging.info(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error("❌ Failed to connect to Kafka after all retries")
                raise e
    
    return None

# ================= MongoDB Config =================
MONGO_URI = "mongodb://mongo-v4:27017"
DB_NAME = "news_db"
COLLECTION_NAME = "articles"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

# Tạo unique index cho URL
collection.create_index("url", unique=True)

# ================= Helper =================
def format_datetime(dt: datetime):
    return dt.strftime("%d/%m/%Y/%H/%M/%S")

def clean_description(desc: str) -> str:
    """Loại bỏ thẻ <a>, <img>, giữ lại nội dung text"""
    if not desc:
        return ""
    soup = BeautifulSoup(desc, "html.parser")
    return soup.get_text().strip()

# ================= RSS Feeds Config =================
RSS_FEEDS = {
    "kenh14": {
        "star": "https://kenh14.vn/star.rss",
        "musics": "https://kenh14.vn/musics.rss",
    }
}

# ================= Crawler Function =================
def crawl_rss(url, source, category):
    logging.info(f"🔍 Crawling RSS feed: {url}")
    
    try:
        feed = feedparser.parse(url)
        logging.info(f"📊 RSS Feed Status: {feed.status if hasattr(feed, 'status') else 'Unknown'}")
        logging.info(f"📰 Number of entries found: {len(feed.entries) if hasattr(feed, 'entries') else 0}")
        
        if not hasattr(feed, 'entries') or not feed.entries:
            logging.warning(f"⚠️ No entries found in RSS feed: {url}")
            return []
        
        docs = []
        for i, entry in enumerate(feed.entries):
            try:
                # Debug: Print entry data
                logging.info(f"📄 Processing entry {i+1}: {entry.get('title', 'No title')}")
                
                # Check if entry has required fields
                if not hasattr(entry, 'link') or not hasattr(entry, 'title'):
                    logging.warning(f"⚠️ Entry {i+1} missing required fields, skipping")
                    continue
                
                doc = {
                    "source": source,
                    "category": category,
                    "url": entry.link,
                    "title": entry.title,
                    "content": clean_description(entry.get("description", "")),
                    "published_at": format_datetime(
                        datetime(*entry.published_parsed[:6])
                    ) if hasattr(entry, "published_parsed") and entry.published_parsed else "",
                    "collected_at": format_datetime(datetime.now())
                }
                
                # Debug: Print the document before adding
                logging.info(f"📝 Document created: {doc['title']} from {doc['url']}")
                docs.append(doc)
                
            except Exception as e:
                logging.error(f"❌ Error processing entry {i+1}: {e}")
                continue
        
        logging.info(f"✅ Successfully processed {len(docs)} articles from {url}")
        return docs
        
    except Exception as e:
        logging.error(f"❌ Error crawling RSS feed {url}: {e}")
        return []

# ================= Streaming Loop =================
def run_streaming(poll_interval=60):
    logging.info(f"🚀 Start crawling Kenh14 RSS feeds every {poll_interval}s ...")
    
    # Create Kafka producer with retry mechanism
    producer = create_kafka_producer()
    if not producer:
        logging.error("❌ Cannot start crawler without Kafka connection")
        return

    while True:
        for source, categories in RSS_FEEDS.items():
            for category, url in categories.items():
                try:
                    docs = crawl_rss(url, source, category)
                    logging.info(f"📦 Found {len(docs)} articles to process")
                    
                    for doc in docs:
                        try:
                            # Debug: Print the document data before processing
                            logging.info(f"🔄 Processing article: {doc['title']}")
                            logging.info(f"📄 Article data: {doc}")
                            
                            # Insert Mongo trước để check trùng
                            result = collection.insert_one(doc)
                            logging.info(f"💾 Inserted to MongoDB: {doc['title']}")

                            # Remove _id field before sending to Kafka (ObjectId is not JSON serializable)
                            doc_for_kafka = doc.copy()
                            if '_id' in doc_for_kafka:
                                del doc_for_kafka['_id']
                            
                            # Nếu insert thành công => gửi Kafka
                            producer.send(KAFKA_TOPIC, value=doc_for_kafka)
                            logging.info(f"📤 Sent to Kafka [{source}/{category}] {doc['title']}")

                        except errors.DuplicateKeyError:
                            logging.debug(f"⚠️ Duplicate skipped: {doc['url']}")
                        except Exception as e:
                            logging.error(f"❌ Error processing article {doc.get('title', 'Unknown')}: {e}")

                except Exception as e:
                    logging.error(f"❌ Error crawling {url}: {e}")

        producer.flush()
        logging.info(f"⏰ Waiting {poll_interval} seconds before next crawl...")
        time.sleep(poll_interval)

# ================= Run =================
if __name__ == "__main__":
    run_streaming(poll_interval=60)  # check mỗi 1 phút
