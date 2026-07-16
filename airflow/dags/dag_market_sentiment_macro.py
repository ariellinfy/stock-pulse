"""
dag_market_sentiment_macro:每日例行排程

流程: CNN Fear & Greed (抓取) → (之後接上清洗與載入)
排程: 每天 7:00(台北時間,對應美股收盤後)
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

sys.path.insert(0, "/opt/airflow/project")
    

def run_fear_greed_scraper(**context):
    from scrapers.fear_greed_client import fetch_fear_greed
    from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME
    from datetime import datetime as dt
    import json

    target_date = context["data_interval_end"].date()
    print(f"處理日期: {target_date}")

    result = fetch_fear_greed(target_date)
    if result is None:
        raise ValueError(f"{target_date} Fear & Greed 抓取失敗或無資料")

    client = get_gcs_client()
    content = json.dumps(result, ensure_ascii=False)
    write_raw_json(client, BUCKET_NAME, "fear_greed", target_date, content)
    print(f"✅ Fear & Greed 成功寫入")


default_args = {
    "owner": "stock-pulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}


with DAG(
    dag_id="DAG_Market_Sentiment_Macro",
    default_args=default_args,
    schedule="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["daily", "sentiment_data"],
) as dag:

    fetch_fear_greed = PythonOperator(
        task_id="fetch_fear_greed_daily",
        python_callable=run_fear_greed_scraper,
    )

    clean_fear_greed = DockerOperator(
        task_id="clean_fear_greed_daily",
        image="stock-pulse-spark",
        api_version="auto",
        auto_remove="success",
        command="/opt/spark/bin/spark-submit /app/spark/jobs/clean_fear_greed_daily.py --date {{ data_interval_end.strftime('%Y-%m-%d') }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source="/home/fy/stock-pulse/secrets", target="/app/secrets", type="bind", read_only=True),
        ],
        environment={
            "GCP_SA_KEY_PATH": "/app/secrets/gcp-sa-key.json",
            "GCP_BUCKET_NAME": "stock-pulse-data-lake",
        },
    )

    fetch_fear_greed >> clean_fear_greed