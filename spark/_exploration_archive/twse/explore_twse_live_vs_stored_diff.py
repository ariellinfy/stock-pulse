"""
[ARCHIVED] 比對 TWSE 即時 API 回傳筆數 vs 我們儲存在 GCS 上的 raw data 筆數,
用來追查特定股票代號(如 '5283')遺漏的原因,並分析遺漏代號的格式規律
(純數字4碼 / 5碼 / 帶字母後綴)。

狀態: 一次性資料稽核腳本,非 Spark job。
若要移動此檔案的位置,請確認下方 sys.path.append 的 .parent 層數
與新的資料夾深度一致(目前假設: 專案根目錄/_exploration_archived/twse/)。
"""

import requests
import json
import sys
import re
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import get_gcs_client, BUCKET_NAME

# 即時重新請求,取得當下的真實筆數
headers = {"User-Agent": "Mozilla/5.0"}
resp = requests.get(
    "https://www.twse.com.tw/exchangeReport/MI_INDEX",
    params={"response": "json", "date": "20251217", "type": "ALLBUT0999"},
    headers=headers,
    timeout=15,
)
live_data = resp.json()
live_rows = live_data["tables"][8]["data"]
print(f"即時 API 回傳筆數: {len(live_rows)}")

live_ids = set(row[0] for row in live_rows)
print(f"'5283' 是否在即時資料裡: {'5283' in live_ids}")

# 對照我們存在 GCS 上的 raw data
client = get_gcs_client()
bucket = client.bucket(BUCKET_NAME)
blob = bucket.blob("raw/twse_daily/dt=2025-12-17/data.json")
stored_content = json.loads(blob.download_as_text())
stored_rows = stored_content["data"]
print(f"\n我們儲存的 raw data 筆數: {len(stored_rows)}")

stored_ids = set(row[0] for row in stored_rows)
print(f"'5283' 是否在我們儲存的資料裡: {'5283' in stored_ids}")

# 找出差異
missing_from_storage = live_ids - stored_ids
print(f"\n即時有、但我們儲存的資料裡沒有的代號數: {len(missing_from_storage)}")
print(f"範例: {list(missing_from_storage)[:15]}")

missing_ids = list(live_ids - stored_ids)  # 完整的差集,不只是前 15 筆

print(f"總遺漏代號數: {len(missing_ids)}")

# 檢查格式規律: 純數字4碼 / 純數字5碼 / 帶字母後綴
pure_4digit = [s for s in missing_ids if re.match(r"^\d{4}$", s)]
pure_5digit = [s for s in missing_ids if re.match(r"^\d{5}$", s)]
with_letter = [s for s in missing_ids if re.search(r"[A-Za-z]", s)]

print(f"純數字4碼: {len(pure_4digit)}")
print(f"純數字5碼: {len(pure_5digit)}")
print(f"帶字母: {len(with_letter)}")
print(f"帶字母範例: {with_letter[:20]}")

# 純數字4碼的這批,才是最值得關注的(這才是我們定義的「一般股票」核心範圍)
print(f"\n純數字4碼範例: {pure_4digit[:20]}")
