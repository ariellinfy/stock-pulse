"""
CNN Fear & Greed Index 爬蟲

資料源: https://production.dataviz.cnn.io/index/fearandgreed/graphdata/{start_date}
風險註記: 非官方端點,無正式文件保證穩定性,需做 schema 驗證,失敗記錄 log 不崩潰。

設計原則: 忠實存下完整回應(含 7 個子指標的原始資料),
          下游(dbt fact_fear_greed)目前只會用到 fear_and_greed 這一段。
"""

import sys
import json
from datetime import date
from pathlib import Path

import requests

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import get_gcs_client, write_raw_json

URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

# 用來驗證回應結構是否符合預期的最小必要欄位
REQUIRED_TOP_LEVEL_KEY = "fear_and_greed"
REQUIRED_SUB_KEYS = {"score", "rating", "timestamp"}


def validate_schema(payload: dict) -> bool:
    """
    最小化的 schema 驗證:確認我們真正需要的 fear_and_greed 區塊存在,
    且該有的欄位都在。不驗證其他子指標(即使少了也不影響核心需求)。
    """
    if REQUIRED_TOP_LEVEL_KEY not in payload:
        print(f"❌ 回應缺少必要欄位: {REQUIRED_TOP_LEVEL_KEY}")
        return False

    fg = payload[REQUIRED_TOP_LEVEL_KEY]
    missing = REQUIRED_SUB_KEYS - set(fg.keys())
    if missing:
        print(f"❌ fear_and_greed 缺少必要子欄位: {missing}")
        return False

    return True


def fetch_fear_greed(start_date: date) -> dict | None:
    """
    抓取從 start_date 到今天的 Fear & Greed 資料(含歷史數列跟即時值)。
    非官方端點,任何失敗都只記錄 log、回傳 None,不拋例外。
    """
    date_str = start_date.isoformat()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        resp = requests.get(f"{URL}/{date_str}", headers=headers, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ CNN Fear & Greed 請求失敗: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ CNN Fear & Greed 回應不是合法 JSON: {e}")
        return None

    if not validate_schema(payload):
        return None

    fg = payload["fear_and_greed"]
    print(f"✅ 取得 Fear & Greed 即時值: score={fg['score']:.2f}, rating={fg['rating']}")

    return payload  # 忠實回傳整包,不篩選欄位


if __name__ == "__main__":
    from datetime import timedelta

    start_date = date.today() - timedelta(days=3)
    result = fetch_fear_greed(start_date)

    if result:
        BUCKET_NAME = "stock-pulse-data-lake"
        client = get_gcs_client()

        content = json.dumps(result, ensure_ascii=False)
        write_raw_json(
            client=client,
            bucket_name=BUCKET_NAME,
            source_name="fear_greed",
            target_date=date.today(),
            content=content,
        )
    else:
        print("⚠️ 無資料可寫入,略過此次上傳")