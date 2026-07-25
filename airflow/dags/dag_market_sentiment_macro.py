"""
dag_market_sentiment_macro:每日例行排程

流程: CNN Fear & Greed (抓取) → (之後接上清洗與載入)
排程: 每天 7:00(台北時間,對應美股收盤後)
"""

import os
import sys
from datetime import datetime, timedelta

from docker.types import Mount
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
)

from shared.alerting import log_success, send_slack_alert

sys.path.insert(0, "/opt/airflow/project")


def run_fear_greed_scraper(**context):
    from scrapers.fear_greed_client import fetch_fear_greed
    from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME
    import json

    # target_date = context["data_interval_end"].date()
    target_date = context["data_interval_end"].in_timezone("Asia/Taipei").date()
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
    "on_failure_callback": send_slack_alert,
    "on_success_callback": log_success,
}


with DAG(
    dag_id="DAG_Market_Sentiment_Macro",
    default_args=default_args,
    schedule="0 7 * * *",
    start_date=datetime(2026, 7, 1),
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
        command="/opt/spark/bin/spark-submit /app/spark/jobs/clean_fear_greed_daily.py --date {{ data_interval_end.in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source="/home/fy/stock-pulse/secrets",
                target="/app/secrets",
                type="bind",
                read_only=True,
            ),
        ],
        environment={
            "GCP_SA_KEY_PATH": "/app/secrets/gcp-sa-key.json",
            "GCP_BUCKET_NAME": "stock-pulse-data-lake",
        },
    )

    project_id = os.environ.get("GCP_PROJECT_ID")

    delete_today_fg_partition = BigQueryDeleteTableOperator(
        task_id="delete_today_fg_partition",
        gcp_conn_id="google_cloud_default",
        deletion_dataset_table=(
            f"{project_id}.stockpulse_staging.raw_fear_greed"
            "${{ data_interval_end.in_timezone('Asia/Taipei').strftime('%Y%m%d') }}"
        ),
        ignore_if_missing=True,
    )

    load_fg_to_bq = GCSToBigQueryOperator(
        task_id="load_fear_greed_to_bq",
        gcp_conn_id="google_cloud_default",
        bucket="stock-pulse-data-lake",
        source_objects=[
            "clean/fear_greed_daily/dt={{ data_interval_end.in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}/*.parquet"
        ],
        destination_project_dataset_table=f"{project_id}.stockpulse_staging.raw_fear_greed",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        extra_config={
            "hivePartitioningOptions": {
                "mode": "AUTO",
                "sourceUriPrefix": "gs://stock-pulse-data-lake/clean/fear_greed_daily/",
            }
        },
    )

    fetch_fear_greed >> clean_fear_greed >> delete_today_fg_partition >> load_fg_to_bq
