"""
最小驗證用 DAG:確認 Airflow 排程機制能正確呼叫我們的爬蟲程式碼。
先求能動,之後再擴充成正式的 DAG_Taiwan_Market。
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/project")


def run_twse_scraper(**context):
    from scrapers.twse_client import fetch_daily_quotes
    from datetime import date

    target_date = date(2026, 7, 9)  # 先寫死一天測試,之後改成用 Airflow 排程日期
    result = fetch_daily_quotes(target_date)

    if result is None:
        raise ValueError(f"{target_date} 抓取失敗或無資料")

    print(f"成功取得 {len(result['data'])} 筆資料")


default_args = {
    "owner": "stock-pulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="twse_daily_test",
    default_args=default_args,
    schedule=None,  # 先不設排程,只手動觸發測試
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_twse_daily",
        python_callable=run_twse_scraper,
    )