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

# Hàm gọi file Tiki_Consumer.py
def run_amazon_consumer():
    import os, sys
    print("🚀 Bắt đầu chạy consumer Amazon_Consumer.py ...")
    print("📂 Current working dir:", os.getcwd())
    print("🐍 Python executable:", sys.executable)

    result = subprocess.run(
        ["python3", "/opt/airflow/crawler/Amazon_Consumer.py"],
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
    dag_id="amazon_consumer_dag",
    default_args=default_args,
    schedule_interval="0 * * * *",  # chạy mỗi giờ, đổi thành @once nếu muốn trigger tay
    catchup=False,
    tags=["amazon", "consumer", "kafka", "minio"],
) as dag:

    consume_task = PythonOperator(
        task_id="consume_amazon_products",
        python_callable=run_amazon_consumer,
    )
