"""
所有 scraper 共用的回傳型態定義。

設計原則:每個 scraper 只負責忠實取得原始資料,不清洗、不轉換;但「這次抓取
到底發生了什麼事」這件事,四個 scraper 過去各自用不同的型態表達(dict|None、
list[dict]|None、自訂 dataclass),呼叫端因此得用三種不同方式判斷成功/失敗。
這裡統一成一種型態,讓呼叫端只需要認識一種介面。
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class FetchStatus(Enum):
    SUCCESS = "success"
    # 確認沒有資料可用,且原因明確、可放心視為「這次本來就不會有資料」,不需要
    # 之後重試。依 scraper 不同,涵蓋的情境不同:
    #   - TWSE/TPEx: 確認為非交易日
    #   - Yahoo: 確認新股/下市/該區間無交易資料
    NO_DATA = "no_data"
    # 請求本身失敗、回應格式不符預期,或無法判斷上述兩種情況——狀態不確定,
    # 需要之後重試(通常由 Airflow retry 或呼叫端的重試機制處理)。
    UNKNOWN_FAILURE = "unknown_failure"


@dataclass
class FetchResult:
    status: FetchStatus
    # dict(twse/tpex/fear_greed)或 list[dict](yahoo/industry),依 scraper 而定
    data: Any = None
