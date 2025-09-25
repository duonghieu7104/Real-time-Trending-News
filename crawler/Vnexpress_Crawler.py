import feedparser
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from pymongo import MongoClient, errors
from datetime import datetime
import json, logging, time

# ========== Logging ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ========== Kafka Config ==========
KAFKA_BOOTSTRAP_SERVERS = "kafka-v4:29092"
KAFKA_TOPIC = "raw_news"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    linger_ms=500,
    batch_size=32768,
    compression_type="gzip"
)

# ========== MongoDB Config ==========
MONGO_URI = "mongodb://mongo-v4:27017"
DB_NAME = "news_db"
COLLECTION_NAME = "articles"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.create_index("url", unique=True)

# ========== Helper ==========
def format_datetime(dt: datetime):
    return dt.strftime("%d/%m/%Y/%H/%M/%S")

def clean_description(raw_html: str) -> str:
    """Bóc text từ description, bỏ thẻ <a>, <img>"""
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup.find_all(["a", "img", "br"]):
        tag.decompose()
    return soup.get_text(strip=True)

# ========== Crawl Function ==========
def crawl_vnexpress(rss_url, category="suc-khoe"):
    feed = feedparser.parse(rss_url)
    docs = []

    for entry in feed.entries:
        raw_desc = entry.get("description", "")
        content = clean_description(raw_desc)

        doc = {
            "source": "vnexpress",
            "category": category,
            "url": entry.link,
            "title": entry.title,
            "content": content,
            "published_at": format_datetime(
                datetime(*entry.published_parsed[:6])
            ) if hasattr(entry, "published_parsed") else "",
            "collected_at": format_datetime(datetime.now())
        }
        docs.append(doc)
    return docs

# ========== Streaming ==========
def run_streaming(rss_feeds, poll_interval=60):
    logging.info(f"🚀 Start VnExpress crawler every {poll_interval}s ...")

    while True:
        for category, url in rss_feeds.items():
            try:
                docs = crawl_vnexpress(url, category)
                for doc in docs:
                    try:
                        collection.insert_one(doc)
                        producer.send(KAFKA_TOPIC, value=doc)
                        logging.info(f"✅ Sent [vnexpress/{category}] {doc['title']}")
                    except errors.DuplicateKeyError:
                        logging.debug(f"⚠️ Duplicate skipped: {doc['url']}")
            except Exception as e:
                logging.error(f"❌ Error crawling {url}: {e}")

        producer.flush()
        time.sleep(poll_interval)

# ========== Run ==========
if __name__ == "__main__":
    RSS_FEEDS = {
        "suc-khoe": "https://vnexpress.net/rss/suc-khoe.rss",
        "the-thao": "https://vnexpress.net/rss/the-thao.rss",
    }
    run_streaming(RSS_FEEDS, poll_interval=60)
