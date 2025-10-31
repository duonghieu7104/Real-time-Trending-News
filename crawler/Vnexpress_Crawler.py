import feedparser
from kafka import KafkaProducer
import json
from datetime import datetime
import time
import logging
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

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

USER_AGENT = "Mozilla/5.0 (compatible; NewsCrawler/1.0; +https://example.com/bot)"
REQUEST_TIMEOUT = 8

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
    "vnexpress": {
        # Trang chủ
        "home": "https://vnexpress.net/rss/tin-moi-nhat.rss",
        
        # Thế giới
        "the-gioi": "https://vnexpress.net/rss/the-gioi.rss",
        "kinh-te": "https://vnexpress.net/rss/kinh-te.rss",
        "giao-duc": "https://vnexpress.net/rss/giao-duc.rss",
        "khoa-hoc": "https://vnexpress.net/rss/khoa-hoc.rss",
        "so-hoa": "https://vnexpress.net/rss/so-hoa.rss",
        "giai-tri": "https://vnexpress.net/rss/giai-tri.rss",
        "the-thao": "https://vnexpress.net/rss/the-thao.rss",
        "phap-luat": "https://vnexpress.net/rss/phap-luat.rss",
        "doi-song": "https://vnexpress.net/rss/doi-song.rss",
        "suc-khoe": "https://vnexpress.net/rss/suc-khoe.rss",
        "du-lich": "https://vnexpress.net/rss/du-lich.rss",
        "oto-xe-may": "https://vnexpress.net/rss/oto-xe-may.rss",
        "y-kien": "https://vnexpress.net/rss/y-kien.rss",
        "tam-su": "https://vnexpress.net/rss/tam-su.rss",
        "cuoi": "https://vnexpress.net/rss/cuoi.rss",
        
        # Chuyên mục
        "thoi-su": "https://vnexpress.net/rss/thoi-su.rss",
        "goc-nhin": "https://vnexpress.net/rss/goc-nhin.rss",
        "the-gioi": "https://vnexpress.net/rss/the-gioi.rss",
        "kinh-te": "https://vnexpress.net/rss/kinh-te.rss",
        "giao-duc": "https://vnexpress.net/rss/giao-duc.rss",
        "khoa-hoc": "https://vnexpress.net/rss/khoa-hoc.rss",
        "so-hoa": "https://vnexpress.net/rss/so-hoa.rss",
        "giai-tri": "https://vnexpress.net/rss/giai-tri.rss",
        "the-thao": "https://vnexpress.net/rss/the-thao.rss",
        "phap-luat": "https://vnexpress.net/rss/phap-luat.rss",
        "doi-song": "https://vnexpress.net/rss/doi-song.rss",
        "suc-khoe": "https://vnexpress.net/rss/suc-khoe.rss",
        "du-lich": "https://vnexpress.net/rss/du-lich.rss",
        "oto-xe-may": "https://vnexpress.net/rss/oto-xe-may.rss",
        "y-kien": "https://vnexpress.net/rss/y-kien.rss",
        "tam-su": "https://vnexpress.net/rss/tam-su.rss",
        "cuoi": "https://vnexpress.net/rss/cuoi.rss"
    }
}

# ================= Crawler Function =================
def crawl_rss(url, source, category):
    logging.info(f"🔍 Crawling RSS feed: {url}")
    
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        logging.info(f"📊 RSS Feed Status: {feed.status if hasattr(feed, 'status') else 'Unknown'}")
        logging.info(f"📰 Number of entries found: {len(feed.entries) if hasattr(feed, 'entries') else 0}")
        
        if not hasattr(feed, 'entries') or not feed.entries:
            logging.warning(f"⚠️ No entries found in RSS feed: {url}")
            return []
        
        docs = []
        for i, entry in enumerate(feed.entries):
            try:
                # Debug: Print entry data (reduced verbosity)
                if i < 3:
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
                
                # Debug: Print a few documents
                if i < 2:
                    logging.debug(f"📝 Document created: {doc['title']} from {doc['url']}")
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
    logging.info(f"🚀 Start crawling VnExpress RSS feeds every {poll_interval}s ...")
    
    producer = create_kafka_producer()
    if not producer:
        logging.error("❌ Cannot start crawler without Kafka connection")
        return

    # We loop to keep the script reusable outside Airflow; Airflow will terminate after ~60s
    while True:
        # Build tasks list
        tasks = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            for source, categories in RSS_FEEDS.items():
                for category, url in categories.items():
                    tasks.append(executor.submit(crawl_rss, url, source, category))

            total_docs = 0
            for future in as_completed(tasks):
                try:
                    docs = future.result()
                    total_docs += len(docs)
                    for doc in docs:
                        try:
                            producer.send(KAFKA_TOPIC, value=doc)
                        except Exception as e:
                            logging.error(f"❌ Kafka send error: {e}")
                except Exception as e:
                    logging.error(f"❌ Feed task error: {e}")

        try:
            producer.flush()
        except Exception:
            pass

        logging.info(f"✅ Cycle completed. Sent ~{total_docs} articles to Kafka. ⏰ Next in {poll_interval}s")
        time.sleep(poll_interval)

# ================= Run =================
if __name__ == "__main__":
    run_streaming(poll_interval=60)  # check mỗi 1 phút