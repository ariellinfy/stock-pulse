"""
零重依賴的環境變數/GCP 憑證工具——只 import os,不 import 任何 google-cloud 套件。

獨立成這個模組(而不是放進 shared/utils.py),是為了讓不需要 GCS/BigQuery
client 的呼叫端也能安全重用這裡的邏輯,不會被迫多裝一個用不到的套件。
例如 streamlit_app 的 Docker image 沒有裝 google-cloud-storage,但
shared/utils.py 頂層有 `from google.cloud import storage`——只要 import
shared.utils 裡任何一個名字,就會執行整個模組頂層、連帶要求那個套件存在。
shared/utils.py 也會從這裡 re-export,兩邊看到的是同一份定義。
"""

import os


def get_required_env(key: str) -> str:
    """
    讀取必要的環境變數,若不存在則立即中斷並給出清楚的錯誤訊息。
    回傳型態明確是 str(不是 str | None),讓呼叫端不需要再處理 None 的情況。
    """
    value = os.environ.get(key)
    if value is None:
        raise RuntimeError(f"環境變數 {key} 未設定,請確認 .env 檔案存在且內容正確")
    return value


def resolve_gcp_credentials(key_path: str | None = None) -> str:
    """
    解析 GCP service account 金鑰路徑,並設定 GOOGLE_APPLICATION_CREDENTIALS
    環境變數,讓 google-cloud-* client 函式庫能自動抓到憑證。

    優先順序: 明確傳入的參數 > 環境變數 GCP_SA_KEY_PATH > 預設相對路徑
    (相對路徑僅為向下相容本機開發習慣,不建議在容器/正式環境依賴它)。
    """
    resolved_path = key_path or os.environ.get(
        "GCP_SA_KEY_PATH", "secrets/gcp-sa-key.json"
    )
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = resolved_path
    return resolved_path
