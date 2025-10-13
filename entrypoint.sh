#!/bin/bash

# Set environment variables
export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-spark-master}
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT:-8080}
export SPARK_WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT:-8081}
export SPARK_WORKER_PORT=${SPARK_WORKER_PORT:-7078}

# Function to start Spark Master
start_master() {
    echo "🚀 Starting Spark Master..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host $SPARK_MASTER_HOST \
        --port $SPARK_MASTER_PORT \
        --webui-port $SPARK_MASTER_WEBUI_PORT
}

# Function to start Spark Worker
start_worker() {
    echo "🚀 Starting Spark Worker..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port $SPARK_WORKER_WEBUI_PORT \
        --port $SPARK_WORKER_PORT \
        $SPARK_MASTER
}

# Main logic
case "${SPARK_MODE:-master}" in
    master)
        start_master
        ;;
    worker)
        start_worker
        ;;
    *)
        echo "❌ Unknown SPARK_MODE: ${SPARK_MODE}"
        exit 1
        ;;
esac
