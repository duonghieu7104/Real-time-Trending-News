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
    "kenh14": {
        # Trang chủ
        "home": "https://kenh14.vn/rss/home.rss",
        
        # Star
        "star": "https://kenh14.vn/star.rss",
        "hoi-ban-than-showbiz": "https://kenh14.vn/rss/star/hoi-ban-than-showbiz.rss",
        "sao-viet": "https://kenh14.vn/rss/star/sao-viet.rss",
        "tv-show": "https://kenh14.vn/rss/star/tv-show.rss",
        
        # Học đường
        "hoc-duong": "https://kenh14.vn/hoc-duong.rss",
        "nhan-vat": "https://kenh14.vn/rss/hoc-duong/nhan-vat.rss",
        "du-hoc": "https://kenh14.vn/rss/hoc-duong/du-hoc.rss",
        "ban-tin-46": "https://kenh14.vn/rss/hoc-duong/ban-tin-46.rss",
        
        # Beauty & Fashion
        "beauty-fashion": "https://kenh14.vn/beauty-fashion.rss",
        "star-style": "https://kenh14.vn/rss/beauty-fashion/star-style.rss",
        "lam-dep": "https://kenh14.vn/rss/beauty-fashion/lam-dep.rss",
        "thoi-trang": "https://kenh14.vn/rss/beauty-fashion/thoi-trang.rss",
        
        # Cine
        "cine": "https://kenh14.vn/cine.rss",
        "phim-chieu-rap": "https://kenh14.vn/rss/cine/phim-chieu-rap.rss",
        "phim-viet-nam": "https://kenh14.vn/rss/cine/phim-viet-nam.rss",
        "series-truyen-hinh": "https://kenh14.vn/rss/cine/series-truyen-hinh.rss",
        "hoa-ngu-han-quoc": "https://kenh14.vn/rss/cine/hoa-ngu-han-quoc.rss",
        
        # Musik
        "musik": "https://kenh14.vn/musik.rss",
        "au-my": "https://kenh14.vn/rss/musik/au-my.rss",
        "chau-a": "https://kenh14.vn/rss/musik/chau-a.rss",
        "viet-nam": "https://kenh14.vn/rss/musik/viet-nam.rss",
        
        # Thế Giới Đó Đây
        "the-gioi-do-day": "https://kenh14.vn/the-gioi-do-day.rss",
        "chum-anh": "https://kenh14.vn/rss/the-gioi-do-day/chum-anh.rss",
        "kham-pha": "https://kenh14.vn/rss/the-gioi-do-day/kham-pha.rss",
        "di": "https://kenh14.vn/rss/the-gioi-do-day/di.rss",
        
        # Đời sống
        "doi-song": "https://kenh14.vn/doi-song.rss",
        "mommy-ez": "https://kenh14.vn/rss/doi-song/mommy-ez.rss",
        "house-n-home": "https://kenh14.vn/rss/doi-song/house-n-home.rss",
        "nhan-vat-doi-song": "https://kenh14.vn/rss/doi-song/nhan-vat.rss",
        
        # Tek-life
        "tek-life": "https://kenh14.vn/tek-life.rss",
        "metaverse": "https://kenh14.vn/rss/tek-life/metaverse.rss",
        "how-to": "https://kenh14.vn/rss/tek-life/how-to.rss",
        "wow": "https://kenh14.vn/rss/tek-life/wow.rss",
        "2-mall": "https://kenh14.vn/rss/tek-life/2-mall.rss",
        
        # Money-Z
        "money-z": "https://kenh14.vn/money-z.rss",
        
        # Xem Mua Luôn
        "xem-mua-luon": "https://kenh14.vn/xem-mua-luon.rss",
        "mommy-mua-di": "https://kenh14.vn/rss/xem-mua-luon/mommy-mua-di.rss",
        "thoi-trang-mua": "https://kenh14.vn/rss/xem-mua-luon/thoi-trang.rss",
        "dep": "https://kenh14.vn/rss/xem-mua-luon/dep.rss",
        
        # Sport
        "sport": "https://kenh14.vn/sport.rss",
        "bong-da": "https://kenh14.vn/rss/sport/bong-da.rss",
        "hau-truong": "https://kenh14.vn/rss/sport/hau-truong.rss",
        "pickleball": "https://kenh14.vn/rss/sport/pickleball.rss",
        "esports": "https://kenh14.vn/rss/sport/esports.rss",
        
        # Ăn - Quẩy - Đi
        "an-quay-di": "https://kenh14.vn/an-quay-di.rss",
        "an": "https://kenh14.vn/rss/an-quay-di/an.rss",
        "quay": "https://kenh14.vn/rss/an-quay-di/quay.rss",
        "di-an-quay": "https://kenh14.vn/rss/an-quay-di/di.rss",
        
        # Xã hội
        "xa-hoi": "https://kenh14.vn/xa-hoi.rss",
        "phap-luat": "https://kenh14.vn/rss/xa-hoi/phap-luat.rss",
        "nong-tren-mang": "https://kenh14.vn/rss/xa-hoi/nong-tren-mang.rss",
        "song-xanh": "https://kenh14.vn/rss/xa-hoi/song-xanh.rss",
        
        # Sức khỏe
        "suc-khoe": "https://kenh14.vn/suc-khoe.rss",
        "tin-tuc-suc-khoe": "https://kenh14.vn/rss/suc-khoe/tin-tuc.rss",
        "khoe-dep": "https://kenh14.vn/rss/suc-khoe/khoe-dep.rss",
        "gioi-tinh": "https://kenh14.vn/rss/suc-khoe/gioi-tinh.rss",
        "cac-benh": "https://kenh14.vn/rss/suc-khoe/cac-benh.rss",
        "dinh-duong": "https://kenh14.vn/rss/suc-khoe/dinh-duong.rss",
        
        # Xem Ăn Chơi
        "xem-an-choi": "https://kenh14.vn/xem-an-choi.rss"
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
    logging.info(f"🚀 Start crawling Kenh14 RSS feeds every {poll_interval}s ...")
    
    producer = create_kafka_producer()
    if not producer:
        logging.error("❌ Cannot start crawler without Kafka connection")
        return

    while True:
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
