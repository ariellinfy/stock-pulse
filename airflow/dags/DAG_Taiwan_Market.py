"""
DAG_Taiwan_Market:每日例行排程

流程: TWSE + TPEx + 產業分類清單(平行抓取)→ (之後接上清洗與載入)
排程: 每天 16:00(台北時間,對應收盤後)
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/project")


def run_twse_scraper(**context):
    from scrapers.twse_client import fetch_daily_quotes_no_permanent_mark  # 需要新增一個不寫永久標記的版本
    from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME
    from datetime import datetime as dt
    import json

    target_date = dt.strptime(context["ds"], "%Y-%m-%d").date()
    print(f"處理日期: {target_date}")

    result = fetch_daily_quotes_no_permanent_mark(target_date)

    if result is None:
        raise ValueError(
            f"{target_date} TWSE 無法取得資料(可能是非交易日,或當日資料尚未彙整完成)。"
            f"若確認是交易日,請檢查排程時間是否過早,或手動重跑此任務。"
        )

    client = get_gcs_client()
    content = json.dumps(result, ensure_ascii=False)
    write_raw_json(client, BUCKET_NAME, "twse_daily", target_date, content)
    print(f"✅ TWSE 成功寫入 {len(result['data'])} 筆資料")
    

def run_tpex_scraper(**context):
    from scrapers.tpex_client import fetch_daily_quotes
    from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME
    from datetime import datetime as dt
    import json

    target_date = dt.strptime(context["ds"], "%Y-%m-%d").date()
    print(f"處理日期: {target_date}")

    # 提醒: TPEx 官方端點固定回傳「當下最新交易日」,不保證等於 target_date
    # 這是 2.2 就確認過的已知限制,例行排程本來就是抓當天,行為上是對的
    result = fetch_daily_quotes(target_date)
    if result is None:
        raise ValueError(f"TPEx 抓取失敗或無資料")

    client = get_gcs_client()
    content = json.dumps(result, ensure_ascii=False)
    write_raw_json(client, BUCKET_NAME, "tpex_daily", target_date, content)
    print(f"✅ TPEx 成功寫入 {len(result['data'])} 筆資料,實際日期: {result.get('actual_trade_date')}")


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
        write_raw_json(client, BUCKET_NAME, f"industry_list_{market.lower()}", today, content)
        print(f"✅ {market} 產業分類清單成功寫入 {len(records)} 筆")


default_args = {
    "owner": "stock-pulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="DAG_Taiwan_Market",
    default_args=default_args,
    schedule="0 17 * * *",  # 每天 16:00(容器已設定 Asia/Taipei 時區)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["daily", "market_data"],
) as dag:

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

    # 三個任務彼此獨立,不互相依賴,可以平行執行(LocalExecutor 支援平行)