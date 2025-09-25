from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# ============ Default config cho DAG ============
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 19),   # chỉnh lại ngày start nếu cần
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
# ===============================================

# Hàm gọi file RSS_Consumer_Continuous.py
def run_rss_consumer():
    import os, sys
    print("🚀 Bắt đầu chạy continuous consumer RSS_Consumer_Continuous.py ...")
    print("📂 Current working dir:", os.getcwd())
    print("🐍 Python executable:", sys.executable)

    result = subprocess.run(
        ["python3", "/opt/airflow/crawler/RSS_Consumer_Continuous.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    print("📜 STDOUT:\n", result.stdout)
    print("📜 STDERR:\n", result.stderr)
    print("🔹 Return code:", result.returncode)

    if result.returncode != 0:
        raise Exception(f"Consumer exited with code {result.returncode}")

# Tạo DAG
with DAG(
    dag_id="rss_consumer_dag",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only - will run continuously
    catchup=False,
    tags=["rss", "consumer", "kafka", "mongodb"],
) as dag:

    consume_task = PythonOperator(
        task_id="consume_rss_news",
        python_callable=run_rss_consumer,
    )
