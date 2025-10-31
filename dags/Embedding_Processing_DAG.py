"""
Embedding Processing DAG
Step 3: Get data from raw_news topic, process with ONNX model, send to processed_data topic
Step 4: Get data from processed_data topic and save to MongoDB
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import subprocess
import logging
import os
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default config
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_model_availability():
    """Check if ONNX model files are available"""
    model_path = "/app/model"
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
            value_serializer=lambda x: x.encode('utf-8')
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
        client = MongoClient("mongodb://mongo-v4:27017")
        client.admin.command('ping')
        client.close()
        logger.info("✅ MongoDB connection successful")
        return True
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        raise e

def start_spark_embedding_processor():
    """Start Spark streaming processor for embedding generation"""
    try:
        logger.info("🚀 Starting Spark streaming embedding processor...")
        
        # First, ensure Spark containers are running
        logger.info("🔧 Ensuring Spark containers are running...")
        
        # Check if spark-master is running, if not start it
        check_master = subprocess.run([
            "docker", "ps", "-q", "-f", "name=spark-master-v4"
        ], capture_output=True, text=True)
        
        if not check_master.stdout.strip():
            logger.info("📦 Starting Spark Master container...")
            try:
                # Try to start existing container
                subprocess.run([
                    "docker", "start", "spark-master-v4"
                ], check=True, capture_output=True)
                logger.info("✅ Spark Master started successfully")
            except subprocess.CalledProcessError:
                logger.warning("⚠️ Spark Master container not found or failed to start")
                logger.info("💡 Please ensure Spark containers are created first")
            import time
            time.sleep(10)  # Wait for master to start
        
        # Check if spark-worker is running, if not start it
        check_worker = subprocess.run([
            "docker", "ps", "-q", "-f", "name=spark-worker-v4"
        ], capture_output=True, text=True)
        
        if not check_worker.stdout.strip():
            logger.info("📦 Starting Spark Worker container...")
            try:
                # Try to start existing container
                subprocess.run([
                    "docker", "start", "spark-worker-v4"
                ], check=True, capture_output=True)
                logger.info("✅ Spark Worker started successfully")
            except subprocess.CalledProcessError:
                logger.warning("⚠️ Spark Worker container not found or failed to start")
                logger.info("💡 Please ensure Spark containers are created first")
            import time
            time.sleep(10)  # Wait for worker to start
        
        logger.info("✅ Spark containers are running")
        
        # Start Spark streaming job in background (run from Spark Master container)
        subprocess.Popen([
            "docker", "exec", "-d", "spark-master-v4",
            "bash", "-lc",
            "nohup spark-submit --master spark://spark-master:7077 --conf spark.jars=/opt/spark/work-dir/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/jars/kafka-clients-3.5.0.jar,/opt/spark/work-dir/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/work-dir/jars/commons-pool2-2.11.1.jar /opt/spark/work-dir/processor/spark_embedding_processor.py > /tmp/embedding_processor.out 2>&1 &"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True)
        
        # Wait a bit then verify inside container
        import time
        time.sleep(15)
        check = subprocess.run([
            "docker", "exec", "spark-master-v4", "bash", "-lc",
            "pgrep -af 'spark-submit|spark_embedding_processor.py' || true"
        ], capture_output=True, text=True)
        if not check.stdout.strip():
            # Show last logs to help debug
            logs = subprocess.run([
                "docker", "exec", "spark-master-v4", "bash", "-lc",
                "tail -n 200 /tmp/embedding_processor.out 2>/dev/null || true"
            ], capture_output=True, text=True)
            logger.error("❌ Spark processor appears not running")
            logger.error(f"📜 LAST LOGS: {logs.stdout}")
            raise Exception("Spark processor failed to start")
        
        logger.info("✅ Spark streaming processor started successfully in background")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to start Spark processor: {e}")
        raise e

def start_processed_data_consumer():
    """Start consumer for processed_data topic"""
    try:
        logger.info("🚀 Starting processed_data consumer...")
        
        # Start consumer in background (no timeout)
        process = subprocess.Popen([
            "bash", "-lc",
            "nohup python3 /opt/airflow/processor/processed_data_consumer.py > /opt/airflow/logs/processed_consumer.out 2>&1 & echo $!"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True)
        
        # Wait a bit to check if it starts successfully
        import time
        time.sleep(8)
        pid_output, _ = process.communicate(timeout=5)
        pid = pid_output.strip()
        # Verify by PID or by process name
        verify = subprocess.run([
            "bash", "-lc",
            f"ps -p {pid} -o pid,cmd= 2>/dev/null || pgrep -af processed_data_consumer.py || true"
        ], capture_output=True, text=True)
        if not verify.stdout.strip():
            last = subprocess.run([
                "bash", "-lc",
                "tail -n 200 /opt/airflow/logs/processed_consumer.out 2>/dev/null || true"
            ], capture_output=True, text=True)
            logger.error("❌ Consumer appears not running")
            logger.error(f"📜 LAST LOGS: {last.stdout}")
            raise Exception("Consumer failed to start")
        
        logger.info("✅ Processed data consumer started successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to start consumer: {e}")
        raise e

def monitor_processing_metrics():
    """Monitor processing metrics and health"""
    try:
        from pymongo import MongoClient
        
        # Check MongoDB
        mongo_client = MongoClient("mongodb://mongo-v4:27017")
        db = mongo_client["news_db"]
        
        # Count documents in different collections
        raw_articles = db["articles"].count_documents({})
        processed_articles = db["processed_articles"].count_documents({})
        
        # Get recent processing stats
        from datetime import datetime, timedelta
        recent_24h = datetime.now() - timedelta(hours=24)
        recent_processed = db["processed_articles"].count_documents({
            "processed_at": {"$gte": recent_24h.strftime("%d/%m/%Y/%H/%M/%S")}
        })
        
        logger.info(f"📊 Processing Stats:")
        logger.info(f"   - Raw articles: {raw_articles}")
        logger.info(f"   - Processed articles: {processed_articles}")
        logger.info(f"   - Recent processed (24h): {recent_processed}")
        logger.info(f"   - Processing rate: {(processed_articles/raw_articles*100):.2f}%" if raw_articles > 0 else "   - No raw articles found")
        
        mongo_client.close()
        logger.info("✅ Monitoring completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Monitoring failed: {e}")
        raise e

# DAG Definition
with DAG(
    dag_id="embedding_processing_dag",
    default_args=default_args,
    description="Embedding processing pipeline: raw_news -> processed_data -> MongoDB",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["embedding", "kafka", "mongodb", "onnx", "spark"],
) as dag:

    # Start task
    start_task = DummyOperator(
        task_id="start_embedding_pipeline",
        doc_md="Start of the embedding processing pipeline"
    )
    
    # Pre-flight checks
    check_model_task = PythonOperator(
        task_id="check_model_availability",
        python_callable=check_model_availability,
        doc_md="Verify ONNX model files are available"
    )
    
    check_kafka_task = PythonOperator(
        task_id="check_kafka_connection",
        python_callable=check_kafka_connection,
        doc_md="Verify Kafka connectivity"
    )
    
    check_mongodb_task = PythonOperator(
        task_id="check_mongodb_connection",
        python_callable=check_mongodb_connection,
        doc_md="Verify MongoDB connectivity"
    )
    
    # Step 3: Spark streaming processor
    spark_processor_task = PythonOperator(
        task_id="start_spark_embedding_processor",
        python_callable=start_spark_embedding_processor,
        doc_md="Start Spark streaming processor: raw_news -> processed_data"
    )
    
    # Step 4: Processed data consumer
    consumer_task = PythonOperator(
        task_id="start_processed_data_consumer",
        python_callable=start_processed_data_consumer,
        doc_md="Start consumer: processed_data -> MongoDB"
    )
    
    # Monitoring task
    monitor_task = PythonOperator(
        task_id="monitor_processing_metrics",
        python_callable=monitor_processing_metrics,
        doc_md="Monitor processing metrics and system health"
    )
    
    # End task
    end_task = DummyOperator(
        task_id="end_embedding_pipeline",
        doc_md="End of the embedding processing pipeline"
    )
    
    # Task dependencies
    start_task >> [check_model_task, check_kafka_task, check_mongodb_task]
    
    [check_model_task, check_kafka_task, check_mongodb_task] >> spark_processor_task
    
    spark_processor_task >> consumer_task
    
    consumer_task >> monitor_task
    
    monitor_task >> end_task
