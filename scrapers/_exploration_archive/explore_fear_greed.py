import requests
import json
from datetime import date, timedelta

URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"


def explore():
    # 抓最近 3 天,範圍小一點方便檢查
    start_date = (date.today() - timedelta(days=3)).isoformat()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    resp = requests.get(f"{URL}/{start_date}", headers=headers, timeout=15)
    print(f"HTTP 狀態碼: {resp.status_code}")
    resp.raise_for_status()

    data = resp.json()

    print("\n=== 最外層 keys ===")
    print(list(data.keys()))

    print("\n=== 完整內容(先整包看,還不假設結構)===")
    print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])  # 先看前 2000 字元


if __name__ == "__main__":
    explore()
