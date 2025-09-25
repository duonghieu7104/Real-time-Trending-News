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

def run_kenh14_producer():
    import os
    import sys
    try:
        print("🚀 Bắt đầu chạy crawler Kenh14_Crawler.py ...")
        print("📂 Current working dir:", os.getcwd())
        print("🐍 Python executable:", sys.executable)
        print("📂 List /opt/airflow/crawler:", os.listdir("/opt/airflow/crawler"))

        result = subprocess.run(
            ["python3", "/opt/airflow/crawler/Kenh14_Crawler.py"],
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
        print("❌ Lỗi trong DAG kenh14_producer_dag")
        raise


with DAG(
    dag_id="kenh14_producer_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["kenh14", "producer", "kafka"],
) as dag:

    run_task = PythonOperator(
        task_id="run_kenh14_producer",
        python_callable=run_kenh14_producer,
    )
