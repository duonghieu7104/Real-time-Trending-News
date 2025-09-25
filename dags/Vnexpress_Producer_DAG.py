import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_vnexpress_producer():
    import os
    import sys
    try:
        print("🚀 Bắt đầu chạy crawler Vnexpress_Crawler.py ...")
        print("📂 Current working dir:", os.getcwd())
        print("🐍 Python executable:", sys.executable)
        print("📂 List /opt/airflow/crawler:", os.listdir("/opt/airflow/crawler"))

        result = subprocess.run(
            ["python3", "/opt/airflow/crawler/Vnexpress_Crawler.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        print("📜 STDOUT:\n", result.stdout)
        print("📜 STDERR:\n", result.stderr)
        print("🔹 Return code:", result.returncode)

        if result.returncode != 0:
            raise Exception(f"Crawler exited with code {result.returncode}")

    except Exception as e:
        print("❌ Lỗi trong DAG vnexpress_producer_dag")
        raise


with DAG(
    dag_id="vnexpress_producer_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["vnexpress", "producer", "kafka"],
) as dag:

    run_task = PythonOperator(
        task_id="run_vnexpress_producer",
        python_callable=run_vnexpress_producer,
    )
