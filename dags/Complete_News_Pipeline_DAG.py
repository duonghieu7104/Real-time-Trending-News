"""
Complete News Pipeline DAG - Real-time Streaming Version
Pipeline chạy định kỳ mỗi 2 phút:
1. Check infrastructure (model, Kafka, MongoDB)
2. Ensure long-running services are active (consumers, Spark processor)
3. Run crawlers for 60s to collect fresh data
4. Monitor metrics

Services chạy liên tục (long-running):
- Raw data consumer (Kafka -> MongoDB)
- Spark embedding processor (Kafka streaming -> processing -> Kafka)
- Processed data consumer (Kafka -> MongoDB)
"""

import subprocess
import logging
import os
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default config
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# ==================== Infrastructure Checks ====================

def check_model_availability():
    """Check if ONNX model files are available"""
    model_path = "/app/model/embedding"
    required_files = ["model.onnx", "config.json", "tokenizer.json"]
    
    for file in required_files:
        file_path = os.path.join(model_path, file)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Required model file not found: {file_path}")
    
    logger.info("✅ All ONNX model files are available")
    return True

def check_kafka_connection():
    """Check Kafka connectivity"""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=['kafka-v4:29092'],
            value_serializer=lambda x: x.encode('utf-8'),
            request_timeout_ms=10000
        )
        producer.close()
        logger.info("✅ Kafka connection successful")
        return True
    except Exception as e:
        logger.error(f"❌ Kafka connection failed: {e}")
        raise e

def check_mongodb_connection():
    """Check MongoDB connectivity"""
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://mongo-v4:27017", serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        client.close()
        logger.info("✅ MongoDB connection successful")
        return True
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        raise e

# ==================== Service Management (Idempotent) ====================

def ensure_spark_containers_running():
    """Ensure Spark Master and Worker containers are running"""
    try:
        logger.info("🔧 Ensuring Spark containers are running...")
        
        # Check if containers exist first
        def check_container_exists(container_name):
            result = subprocess.run([
                "docker", "ps", "-a", "-q", "-f", f"name={container_name}"
            ], capture_output=True, text=True, timeout=10)
            return result.stdout.strip()
        
        # Check and start Spark Master
        master_exists = check_container_exists("spark-master-v4")
        if not master_exists:
            logger.error("❌ Spark Master container does not exist. Please run docker-compose up first.")
            raise Exception("Spark Master container not found")
        
        check_master = subprocess.run([
            "docker", "ps", "-q", "-f", "name=spark-master-v4"
        ], capture_output=True, text=True, timeout=10)
        
        if not check_master.stdout.strip():
            logger.info("📦 Starting Spark Master container...")
            result = subprocess.run(["docker", "start", "spark-master-v4"], 
                                 capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                logger.error(f"❌ Failed to start Spark Master: {result.stderr}")
                raise Exception(f"Failed to start Spark Master: {result.stderr}")
            time.sleep(8)
        else:
            logger.info("✓ Spark Master already running")
        
        # Check and start Spark Worker
        worker_exists = check_container_exists("spark-worker-v4")
        if not worker_exists:
            logger.error("❌ Spark Worker container does not exist. Please run docker-compose up first.")
            raise Exception("Spark Worker container not found")
        
        check_worker = subprocess.run([
            "docker", "ps", "-q", "-f", "name=spark-worker-v4"
        ], capture_output=True, text=True, timeout=10)
        
        if not check_worker.stdout.strip():
            logger.info("📦 Starting Spark Worker container...")
            result = subprocess.run(["docker", "start", "spark-worker-v4"], 
                                 capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                logger.error(f"❌ Failed to start Spark Worker: {result.stderr}")
                raise Exception(f"Failed to start Spark Worker: {result.stderr}")
            time.sleep(8)
        else:
            logger.info("✓ Spark Worker already running")
        
        logger.info("✅ Spark containers are running")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("❌ Timeout while checking/starting Spark containers")
        raise Exception("Timeout while managing Spark containers")
    except Exception as e:
        logger.error(f"❌ Failed to ensure Spark containers: {e}")
        raise e

def ensure_raw_data_consumer_running():
    """Ensure raw_news consumer is running (idempotent)"""
    try:
        # Check if already running
        check = subprocess.run([
            "docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
            "ps aux | grep -E 'raw_data_consumer.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)
        
        if check.stdout.strip() and int(check.stdout.strip()) > 0:
            logger.info("✓ Raw data consumer already running")
            return True
        
        logger.info("🚀 Starting raw_news consumer...")
        
        # Start consumer in background
        subprocess.Popen([
            "docker", "exec", "-d", "airflow-scheduler-v4",
            "bash", "-lc",
            "cd /opt/airflow && nohup python processor/raw_data_consumer.py > /tmp/raw_consumer.out 2>&1 &"
        ])
        
        # Wait and verify
        time.sleep(8)
        verify = subprocess.run([
            "docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
            "ps aux | grep -E 'raw_data_consumer.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)
        
        if not verify.stdout.strip() or int(verify.stdout.strip()) == 0:
            logger.error("❌ Raw consumer failed to start")
            raise Exception("Raw consumer failed to start")
        
        logger.info("✅ Raw data consumer started successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to ensure raw consumer: {e}")
        raise e

def ensure_spark_processor_running():
    """Ensure Spark embedding processor is running (idempotent)"""
    try:
        # Check if already running inside Spark Master
        check = subprocess.run([
            "docker", "exec", "spark-master-v4", "bash", "-lc",
            "ps aux | grep -E 'working_embedding_processor.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)
        
        if check.stdout.strip() and int(check.stdout.strip()) > 0:
            logger.info("✓ Spark embedding processor already running")
            return True
        
        logger.info("🚀 Starting Spark streaming embedding processor...")
        
        # Start Spark streaming job in background (using working processor)
        subprocess.Popen([
            "docker", "exec", "-d", "spark-master-v4",
            "bash", "-lc",
            "nohup /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars=/opt/spark/work-dir/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/jars/kafka-clients-3.5.0.jar,/opt/spark/work-dir/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/jars/commons-pool2-2.11.1.jar /opt/spark/work-dir/processor/working_embedding_processor.py > /tmp/working_processor.out 2>&1 &"
        ])
        
        # Wait and verify
        time.sleep(12)
        verify = subprocess.run([
            "docker", "exec", "spark-master-v4", "bash", "-lc",
            "ps aux | grep -E 'working_embedding_processor.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)
        
        if not verify.stdout.strip() or int(verify.stdout.strip()) == 0:
            logger.error("❌ Spark processor failed to start")
            raise Exception("Spark processor failed to start")
        
        logger.info("✅ Spark streaming processor started successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to ensure Spark processor: {e}")
        raise e

def report_embedding_status():
    """Report embedding processor running state and VN_WORDSEG status"""
    try:
        # Find processor PID
        find_pid = subprocess.run([
            "docker", "exec", "spark-master-v4", "bash", "-lc",
            "pgrep -f 'working_embedding_processor.py' | head -n1 || true"
        ], capture_output=True, text=True)
        pid = find_pid.stdout.strip()
        if not pid:
            logger.warning("⚠️ Embedding processor not running")
            return True
        
        # Inspect environment of the process to check VN_WORDSEG
        check_env = subprocess.run([
            "docker", "exec", "spark-master-v4", "bash", "-lc",
            f"tr '\\0' '\\n' </proc/{pid}/environ | grep -E '^VN_WORDSEG=' || true"
        ], capture_output=True, text=True)
        env_line = check_env.stdout.strip()
        if env_line:
            logger.info(f"🔧 Embedding processor env: {env_line}")
        else:
            logger.info("🔧 Embedding processor env: VN_WORDSEG not found (segmentation likely disabled)")
        
        logger.info("📣 Embedding processor status reported")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to report embedding status: {e}")
        # Do not fail the DAG due to status reporting
        return True

def ensure_processed_data_consumer_running():
    """Ensure processed_data consumer is running (idempotent)"""
    try:
        # Check if already running
        check = subprocess.run([
            "docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
            "ps aux | grep -E 'processed_data_consumer.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)
        
        if check.stdout.strip() and int(check.stdout.strip()) > 0:
            logger.info("✓ Processed data consumer already running")
            return True
        
        logger.info("🚀 Starting processed_data consumer...")
        
        # Start consumer in background
        subprocess.Popen([
            "docker", "exec", "-d", "airflow-scheduler-v4",
            "bash", "-lc",
            "cd /opt/airflow && nohup python processor/processed_data_consumer.py > /tmp/processed_consumer.out 2>&1 &"
        ])
        
        # Wait and verify
        time.sleep(8)
        verify = subprocess.run([
            "docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
            "ps aux | grep -E 'processed_data_consumer.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)
        
        if not verify.stdout.strip() or int(verify.stdout.strip()) == 0:
            logger.error("❌ Processed consumer failed to start")
            raise Exception("Processed consumer failed to start")
        
        logger.info("✅ Processed data consumer started successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to ensure processed consumer: {e}")
        raise e

# ==================== Crawlers (long-running ensures) ====================

def ensure_vnexpress_crawler_running():
    """Ensure VnExpress crawler is running continuously (idempotent)"""
    try:
        # Check if already running
        check = subprocess.run([
            "docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
            "ps aux | grep -E 'Vnexpress_Crawler.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)

        if check.stdout.strip() and int(check.stdout.strip()) > 0:
            logger.info("✓ VnExpress crawler already running")
            return True

        logger.info("🚀 Starting VnExpress crawler (continuous)...")
        # Start crawler in background
        subprocess.Popen([
            "docker", "exec", "-d", "airflow-scheduler-v4",
            "bash", "-lc",
            "cd /opt/airflow && nohup python3 crawler/Vnexpress_Crawler.py > /tmp/vnexpress_crawler.out 2>&1 &"
        ])

        time.sleep(8)
        # Verify it's running
        verify = subprocess.run([
            "docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
            "ps aux | grep -E 'Vnexpress_Crawler.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)

        if not verify.stdout.strip() or int(verify.stdout.strip()) == 0:
            logger.error("❌ VnExpress crawler failed to start")
            raise Exception("VnExpress crawler failed to start")

        logger.info("✅ VnExpress crawler started successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to ensure VnExpress crawler: {e}")
        raise e

def ensure_kenh14_crawler_running():
    """Ensure Kenh14 crawler is running continuously (idempotent)"""
    try:
        # Check if already running
        check = subprocess.run([
            "docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
            "ps aux | grep -E 'Kenh14_Crawler.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)

        if check.stdout.strip() and int(check.stdout.strip()) > 0:
            logger.info("✓ Kenh14 crawler already running")
            return True

        logger.info("🚀 Starting Kenh14 crawler (continuous)...")
        # Start crawler in background
        subprocess.Popen([
            "docker", "exec", "-d", "airflow-scheduler-v4",
            "bash", "-lc",
            "cd /opt/airflow && nohup python3 crawler/Kenh14_Crawler.py > /tmp/kenh14_crawler.out 2>&1 &"
        ])

        time.sleep(8)
        # Verify it's running
        verify = subprocess.run([
            "docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
            "ps aux | grep -E 'Kenh14_Crawler.py' | grep -v grep | wc -l"
        ], capture_output=True, text=True)

        if not verify.stdout.strip() or int(verify.stdout.strip()) == 0:
            logger.error("❌ Kenh14 crawler failed to start")
            raise Exception("Kenh14 crawler failed to start")

        logger.info("✅ Kenh14 crawler started successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to ensure Kenh14 crawler: {e}")
        raise e

# ==================== Monitoring ====================

def monitor_processing_metrics():
    """Monitor processing metrics and health"""
    try:
        from pymongo import MongoClient
        
        # Check MongoDB
        mongo_client = MongoClient("mongodb://mongo-v4:27017", serverSelectionTimeoutMS=10000)
        db = mongo_client["news_db"]
        
        # Count documents in different collections (with error handling)
        try:
            raw_articles = db["articles"].count_documents({})
        except Exception:
            raw_articles = 0
            logger.warning("⚠️ Could not count raw articles (collection may not exist)")
        
        try:
            processed_articles = db["processed_articles"].count_documents({})
        except Exception:
            processed_articles = 0
            logger.warning("⚠️ Could not count processed articles (collection may not exist)")
        
        # Get recent processing stats (last 5 minutes - since last DAG run)
        recent_5m = datetime.now() - timedelta(minutes=5)
        recent_5m_str = recent_5m.strftime("%d/%m/%Y/%H/%M/%S")
        
        try:
            recent_raw = db["articles"].count_documents({
                "collected_at": {"$gte": recent_5m_str}
            })
        except Exception:
            recent_raw = 0
            logger.warning("⚠️ Could not count recent raw articles")
        
        try:
            recent_processed = db["processed_articles"].count_documents({
                "processed_at": {"$gte": recent_5m_str}
            })
        except Exception:
            recent_processed = 0
            logger.warning("⚠️ Could not count recent processed articles")
        
        logger.info(f"📊 Processing Stats:")
        logger.info(f"   - Total raw articles: {raw_articles}")
        logger.info(f"   - Total processed articles: {processed_articles}")
        logger.info(f"   - Recent raw (5m): {recent_raw}")
        logger.info(f"   - Recent processed (5m): {recent_processed}")
        if raw_articles > 0:
            logger.info(f"   - Overall processing rate: {(processed_articles/raw_articles*100):.2f}%")
        else:
            logger.info("   - No raw articles found yet")
        
        mongo_client.close()
        logger.info("✅ Monitoring completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Monitoring failed: {e}")
        # Don't fail the DAG due to monitoring issues
        logger.info("⚠️ Continuing despite monitoring failure")
        return True

# ==================== DAG Definition ====================

with DAG(
    dag_id="complete_news_pipeline_dag",
    default_args=default_args,
    description="Real-time news pipeline: crawlers và consumers chạy liên tục, DAG kiểm tra mỗi 1 phút",
    schedule_interval=timedelta(minutes=1),  # Run every 1 minute
    catchup=False,
    max_active_runs=1,
    tags=["real-time", "news", "pipeline", "streaming"],
) as dag:

    # Start task
    start = DummyOperator(
        task_id="start",
        doc_md="Start pipeline run"
    )
    
    # Step 1: Infrastructure checks (parallel)
    check_model = PythonOperator(
        task_id="check_model_availability",
        python_callable=check_model_availability,
        doc_md="Verify ONNX model files are available"
    )
    
    check_kafka = PythonOperator(
        task_id="check_kafka_connection",
        python_callable=check_kafka_connection,
        doc_md="Verify Kafka connectivity"
    )
    
    check_mongodb = PythonOperator(
        task_id="check_mongodb_connection",
        python_callable=check_mongodb_connection,
        doc_md="Verify MongoDB connectivity"
    )
    
    # Step 2: Ensure Spark containers are running
    ensure_spark = PythonOperator(
        task_id="ensure_spark_containers_running",
        python_callable=ensure_spark_containers_running,
        doc_md="Ensure Spark Master and Worker are running"
    )
    
    # Step 3: Ensure all long-running services are active (parallel, idempotent)
    ensure_raw_consumer = PythonOperator(
        task_id="ensure_raw_data_consumer",
        python_callable=ensure_raw_data_consumer_running,
        doc_md="Ensure raw_news consumer is running (idempotent)"
    )
    
    ensure_spark_processor = PythonOperator(
        task_id="start_embedding_processor",
        python_callable=ensure_spark_processor_running,
        doc_md="Ensure Spark embedding processor is running (idempotent)"
    )
    
    ensure_processed_consumer = PythonOperator(
        task_id="ensure_processed_data_consumer",
        python_callable=ensure_processed_data_consumer_running,
        doc_md="Ensure processed_data consumer is running (idempotent)"
    )
    
    # Step 4: Ensure crawlers are running continuously (parallel)
    start_vnexpress_crawler = PythonOperator(
        task_id="ensure_vnexpress_crawler",
        python_callable=ensure_vnexpress_crawler_running,
        doc_md="Ensure VnExpress crawler is running (idempotent)"
    )
    
    start_kenh14_crawler = PythonOperator(
        task_id="ensure_kenh14_crawler",
        python_callable=ensure_kenh14_crawler_running,
        doc_md="Ensure Kenh14 crawler is running (idempotent)"
    )
    
    # Embedding status node (visible in graph)
    embedding_status = PythonOperator(
        task_id="embedding_status",
        python_callable=report_embedding_status,
        doc_md="Report embedding processor running state and segmentation flag"
    )
    
    # Step 5: Monitor metrics
    monitor = PythonOperator(
        task_id="monitor_metrics",
        python_callable=monitor_processing_metrics,
        doc_md="Monitor processing metrics and system health"
    )
    
    # End task
    end = DummyOperator(
        task_id="end",
        doc_md="End pipeline run"
    )
    
    # ==================== Task Dependencies ====================
    # 1) Start -> check infrastructure (parallel)
    start >> [check_model, check_kafka, check_mongodb]
    
    # 2) All checks OK -> ensure Spark containers running
    [check_model, check_kafka, check_mongodb] >> ensure_spark
    
    # 3) Spark ready -> ensure all services running (parallel, idempotent)
    ensure_spark >> [ensure_raw_consumer, ensure_processed_consumer, ensure_spark_processor]
    
    # 4) Start crawlers once raw consumer is ready (crawlers only need raw consumer)
    ensure_raw_consumer >> [start_vnexpress_crawler, start_kenh14_crawler]
    
    # Embedding processor status is reported independently and does not gate crawlers
    ensure_spark_processor >> embedding_status
    
    # 5) After ensuring services, run monitor -> end
    [start_vnexpress_crawler, start_kenh14_crawler] >> monitor >> end
