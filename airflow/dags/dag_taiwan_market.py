"""
dag_taiwan_market:每日例行排程

流程: TWSE + TPEx + 產業分類清單(平行抓取)→ (之後接上清洗與載入)
排程: 每天 17:00(台北時間,對應收盤後)
"""

import os
import sys
from datetime import datetime, timedelta

from docker.types import Mount
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
)

from shared.alerting import log_success, send_slack_alert

sys.path.insert(0, "/opt/airflow/project")


def check_trading_day(**context):
    target_date = context["data_interval_end"].in_timezone("Asia/Taipei").date()
    is_weekend = target_date.weekday() >= 5  # 5=星期六, 6=星期日
    if is_weekend:
        print(f"{target_date} 是週末,跳過本次排程,不浪費時間重試")
        return False
    return True


def run_twse_scraper(**context):
    from scrapers.twse_client import (
        fetch_daily_quotes,
        FetchStatus,
    )  # 需要新增一個不寫永久標記的版本
    from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME
    import json

    target_date = context["data_interval_end"].in_timezone("Asia/Taipei").date()
    print(f"處理日期: {target_date}")

    result = fetch_daily_quotes(target_date)

    if result.status == FetchStatus.NO_TRADING_DAY:
        print(f"ℹ️ {target_date} 確認為非交易日,無需處理")
        return

    if result.status == FetchStatus.UNKNOWN_FAILURE:
        raise ValueError(
            f"{target_date} TWSE 無法取得資料(可能是非交易日,或當日資料尚未彙整完成)。"
            f"若確認是交易日,請檢查排程時間是否過早,或手動重跑此任務。"
        )

    assert result.data is not None  # status == SUCCESS,data 保證有值

    if not result.data.get("fields_match_expected", True):
        # 不直接 raise 讓任務失敗(資料本身還是有效、可以繼續處理),
        # 但這是需要人工關注的訊號,單獨觸發告警
        raise ValueError(
            f"⚠️ {target_date} TWSE 欄位結構與預期不符!"
            f"實際欄位: {result.data.get('fields')}。"
            f"這代表 TWSE API 可能已調整格式,需要人工檢查並更新 schema 定義。"
        )

    client = get_gcs_client()
    content = json.dumps(result, ensure_ascii=False)
    write_raw_json(client, BUCKET_NAME, "twse_daily", target_date, content)
    print(f"✅ TWSE 成功寫入 {len(result.data['data'])} 筆資料")


def run_tpex_scraper(**context):
    from scrapers.tpex_client import fetch_daily_quotes
    from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME
    import json

    target_date = context["data_interval_end"].in_timezone("Asia/Taipei").date()
    print(f"處理日期: {target_date}")

    # 提醒: TPEx 官方端點固定回傳「當下最新交易日」,不保證等於 target_date
    # 這是 2.2 就確認過的已知限制,例行排程本來就是抓當天,行為上是對的
    result = fetch_daily_quotes(target_date)
    if result is None:
        raise ValueError(f"TPEx 抓取失敗或無資料")

    if not result.get("fields_match_expected", True):
        # 不直接 raise 讓任務失敗(資料本身還是有效、可以繼續處理),
        # 但這是需要人工關注的訊號,單獨觸發告警
        raise ValueError(
            f"⚠️ {target_date} TPEX 欄位結構與預期不符!"
            f"實際欄位: {result.get('fields')}。"
            f"這代表 TPEX API 可能已調整格式,需要人工檢查並更新 schema 定義。"
        )

    client = get_gcs_client()
    content = json.dumps(result, ensure_ascii=False)
    write_raw_json(client, BUCKET_NAME, "tpex_daily", target_date, content)
    print(
        f"✅ TPEx 成功寫入 {len(result['data'])} 筆資料,實際日期: {result.get('actual_trade_date')}"
    )


def run_industry_scraper(**context):
    from scrapers.industry_client import fetch_industry_list
    from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME
    from datetime import date
    import json

    client = get_gcs_client()
    today = date.today()

    for market in ("TWSE", "TPEx"):
        records = fetch_industry_list(market)
        if records is None:
            raise ValueError(f"{market} 產業分類清單抓取失敗")

        content = json.dumps(records, ensure_ascii=False)
        write_raw_json(
            client, BUCKET_NAME, f"industry_list_{market.lower()}", today, content
        )
        print(f"✅ {market} 產業分類清單成功寫入 {len(records)} 筆")


default_args = {
    "owner": "stock-pulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_slack_alert,
    "on_success_callback": log_success,
}


with DAG(
    dag_id="DAG_Taiwan_Market",
    default_args=default_args,
    schedule="0 17 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["daily", "market_data"],
) as dag:

    check_trading_day_task = ShortCircuitOperator(
        task_id="check_trading_day",
        python_callable=check_trading_day,
    )

    fetch_twse = PythonOperator(
        task_id="fetch_twse_daily",
        python_callable=run_twse_scraper,
    )

    fetch_tpex = PythonOperator(
        task_id="fetch_tpex_daily",
        python_callable=run_tpex_scraper,
    )

    fetch_industry = PythonOperator(
        task_id="fetch_industry_list",
        python_callable=run_industry_scraper,
    )

    clean_industry = DockerOperator(
        task_id="clean_industry_list",
        image="stock-pulse-spark",
        api_version="auto",
        auto_remove="success",
        command="/opt/spark/bin/spark-submit /app/spark/jobs/clean_industry_list.py",
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

    clean_daily = DockerOperator(
        task_id="clean_daily_stock_data",
        image="stock-pulse-spark",
        api_version="auto",
        auto_remove="success",  # 任務成功後自動清除容器,避免堆積
        command="/opt/spark/bin/spark-submit /app/spark/jobs/clean_stock_daily.py --date {{ data_interval_end.in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}",
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

    delete_today_stock_partition = BigQueryDeleteTableOperator(
        task_id="delete_today_stock_partition",
        gcp_conn_id="google_cloud_default",
        deletion_dataset_table=(
            f"{project_id}.stockpulse_staging.raw_stock_daily"
            "${{ data_interval_end.in_timezone('Asia/Taipei').strftime('%Y%m%d') }}"
        ),
        ignore_if_missing=True,
    )

    load_stock_to_bq = GCSToBigQueryOperator(
        task_id="load_stock_daily_to_bq",
        gcp_conn_id="google_cloud_default",
        bucket="stock-pulse-data-lake",
        source_objects=[
            "clean/stock_daily/dt={{ data_interval_end.in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}/*.parquet"
        ],
        destination_project_dataset_table=f"{project_id}.stockpulse_staging.raw_stock_daily",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        extra_config={
            "hivePartitioningOptions": {
                "mode": "AUTO",
                "sourceUriPrefix": "gs://stock-pulse-data-lake/clean/stock_daily/",
            }
        },
    )

    run_dbt = DockerOperator(
        task_id="run_dbt_models",
        image="stock-pulse-dbt",
        api_version="auto",
        auto_remove="success",
        command="dbt run",
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
    )

    # 判斷是否為平日(週末跳過)
    check_trading_day_task >> [fetch_twse, fetch_tpex, fetch_industry]
    # 產業清單抓取+清洗 必須在 TWSE/TPEx 清洗之前完成(過濾要用最新清單)
    fetch_industry >> clean_industry >> clean_daily
    [fetch_twse, fetch_tpex] >> clean_daily
    # 刪除今天分區(避免重複)+載入BQ
    clean_daily >> delete_today_stock_partition >> load_stock_to_bq >> run_dbt
