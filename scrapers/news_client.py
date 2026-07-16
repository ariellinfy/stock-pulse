"""
財經新聞抓取腳本 v4
來源:
  1. 鉅亨網 JSON API  — https://api.cnyes.com/media/api/v1/newslist/category/tw_stock
  2. Yahoo Finance    — yfinance 內建 .news，按股票代號抓相關新聞
"""

import sys
import re
import time
import hashlib
import requests
import yfinance as yf
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from scrapers._exploration_archive.utils import get_logger, save_json, RAW_DIR, ts_str, today_str

logger = get_logger("news_client")

# ── 設定 ─────────────────────────────────────────────────

CNYES_API_URL = "https://api.cnyes.com/media/api/v1/newslist/category/tw_stock"
CNYES_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://news.cnyes.com/",
    "Accept": "application/json",
}

YFINANCE_SYMBOLS    = ["2330.TW", "2317.TW", "2454.TW", "0050.TW"]
MAX_NEWS_PER_SYMBOL = 10


# ── 工具函式 ─────────────────────────────────────────────

def make_article_id(val: str) -> str:
    return hashlib.md5(val.encode()).hexdigest()[:12]


def unix_to_iso(ts: int | float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def strip_html(text: str) -> str:
    """移除 HTML tag，保留純文字"""
    return re.sub(r"<[^>]+>", "", text or "").strip()


def parse_market_codes(market_list: list) -> list[str]:
    """
    從 market 欄位提取股票代號。
    market 格式: [{"code": "5508", "name": "永信建", "symbol": "TWS:5508:STOCK"}, ...]
    回傳: ["5508", "3228", ...]（台股四位數代號）
    防禦：若 item 不是 dict 則跳過
    """
    codes = []
    for item in (market_list or []):
        if not isinstance(item, dict):
            continue
        code = item.get("code", "")
        if code:
            codes.append(str(code))
    return codes


def parse_other_products(product_list: list) -> list[str]:
    """
    從 otherProduct 欄位提取股票代號。
    格式: ["TWS:5508:STOCK:COMMON", "USS:META:STOCK:COMMON", ...]
    規則: 取冒號第二段
    回傳: ["5508", "META", ...]
    """
    codes = []
    skip = {"US", "USS", "TW", "TWS", "STOCK", "COMMON", "ETF"}
    for item in (product_list or []):
        if not isinstance(item, str):
            continue
        parts = item.split(":")
        if len(parts) >= 2:
            code = parts[1].strip()
            if code and code not in skip:
                codes.append(code)
    return codes


def dedup_list(items: list) -> list:
    seen = set()
    return [x for x in items if x and not (x in seen or seen.add(x))]


# ── 來源 1: 鉅亨網 JSON API ───────────────────────────────

def fetch_cnyes_news(retries: int = 3) -> list[dict]:
    params = {"page": 1, "limit": 30}

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"[鉅亨網] 抓取中（第 {attempt} 次）...")
            resp = requests.get(
                CNYES_API_URL,
                params=params,
                headers=CNYES_HEADERS,
                timeout=20,
            )
            resp.raise_for_status()
            payload = resp.json()

            # 鉅亨 API 實際結構:
            # {"items": {"total": 741, "data": [ {...}, ... ]}}
            items = (
                payload.get("items", {}).get("data")   # 實測結構 ✓
                or payload.get("data", {}).get("items")
                or payload.get("data")
                or []
            )

            if not items:
                logger.warning(f"[鉅亨網] 回傳結構: {list(payload.keys())}")
                return []
            
            total = payload.get("items", {}).get("total", "?")
            logger.info(f"[鉅亨網] 共 {total} 筆新聞，本次取前 {len(items)} 筆")

            articles = []
            for item in items:
                if not isinstance(item, dict):
                    continue

                news_id  = str(item.get("newsId", ""))
                title    = (item.get("title") or "").strip()
                summary  = (item.get("summary") or "").strip()
                content  = strip_html(item.get("content") or "")
                pub_ts   = item.get("publishAt") or 0

                # ── related_stocks: 合併三個欄位 ──────────
                # 1. market → 台股四碼代號（最可靠）
                market_codes = parse_market_codes(item.get("market") or [])
                # 2. otherProduct → 冒號第二段
                other_codes  = parse_other_products(item.get("otherProduct") or [])
                # 3. stock 欄位（部分文章有，格式 "US-META"）
                stock_raw    = item.get("stock") or []
                stock_codes  = parse_other_products(
                    [s.replace("-", ":X:") for s in stock_raw if isinstance(s, str)]
                )

                related_stocks = dedup_list(market_codes + other_codes + stock_codes)

                # ── keyword 去重 ───────────────────────────
                keywords = dedup_list(item.get("keyword") or [])

                if not title:
                    continue

                articles.append({
                    "article_id":      news_id or make_article_id(title),
                    "title":           title,
                    "summary":         summary,
                    "content":         content[:2000],
                    "url":             f"https://news.cnyes.com/news/id/{news_id}",
                    "published_at":    unix_to_iso(pub_ts) if pub_ts else today_str(),
                    "category":        item.get("categoryName") or "",
                    "keyword":         keywords,
                    "related_stocks":  related_stocks,
                    "source_name":     "cnyes",
                    "source_display":  "鉅亨網",
                    "_fetched_date":   today_str(),
                    "_fetched_ts":     ts_str(),
                    "_source":         "cnyes_api",
                    "sentiment_score": None,
                    "sentiment_label": None,
                })

            logger.info(f"[鉅亨網] 取得 {len(articles)} 筆")
            return articles

        except Exception as e:
            logger.error(f"[鉅亨網] 第 {attempt} 次失敗: {e}")
            if attempt < retries:
                time.sleep(3)

    return []


# ── 來源 2: Yahoo Finance 個股新聞 ────────────────────────

def fetch_yfinance_news(symbols: list[str] = YFINANCE_SYMBOLS) -> list[dict]:
    articles = []
    seen_ids: set = set()

    for symbol in symbols:
        try:
            logger.info(f"[Yahoo News] 抓取 {symbol} 新聞...")
            ticker = yf.Ticker(symbol)
            news   = ticker.news or []
            count  = 0

            for item in news[:MAX_NEWS_PER_SYMBOL]:
                content      = item.get("content") or {}
                url          = (
                    content.get("canonicalUrl", {}).get("url")
                    or item.get("link", "")
                )
                title        = (content.get("title") or item.get("title") or "").strip()
                summary      = (content.get("summary") or item.get("summary") or "").strip()
                pub_raw      = content.get("pubDate") or item.get("providerPublishTime") or 0
                published_at = (
                    unix_to_iso(pub_raw)
                    if isinstance(pub_raw, (int, float)) and pub_raw > 0
                    else today_str()
                )

                if not title or not url:
                    continue

                article_id = make_article_id(url)
                if article_id in seen_ids:
                    continue
                seen_ids.add(article_id)

                # 台股代號去掉 .TW / .TWO
                clean_symbol = re.sub(r"\.(TW|TWO)$", "", symbol)

                articles.append({
                    "article_id":      article_id,
                    "title":           title,
                    "summary":         summary[:500],
                    "content":         "",
                    "url":             url,
                    "published_at":    published_at,
                    "category":        "tw_stock",
                    "keyword":         [],
                    "related_stocks":  [clean_symbol],
                    "source_name":     "yahoo_finance_news",
                    "source_display":  f"Yahoo Finance ({symbol})",
                    "_fetched_date":   today_str(),
                    "_fetched_ts":     ts_str(),
                    "_source":         "yfinance_news",
                    "sentiment_score": None,
                    "sentiment_label": None,
                })
                count += 1

            logger.info(f"  ✓ {symbol}: {count} 筆新聞")
            time.sleep(0.5)

        except Exception as e:
            logger.error(f"  ✗ {symbol} 新聞抓取失敗: {e}")

    return articles


# ── 主流程 ───────────────────────────────────────────────

def run() -> Path | None:
    all_articles: list[dict] = []

    cnyes_articles = fetch_cnyes_news()
    all_articles.extend(cnyes_articles)

    yf_articles = fetch_yfinance_news()
    all_articles.extend(yf_articles)

    if not all_articles:
        logger.error("所有新聞來源均失敗，本次不儲存。")
        return None

    # 跨來源去重
    seen: set = set()
    unique = []
    for a in all_articles:
        if a["article_id"] not in seen:
            seen.add(a["article_id"])
            unique.append(a)

    dup_removed = len(all_articles) - len(unique)
    logger.info(f"合計 {len(unique)} 筆（移除 {dup_removed} 筆重複）")

    filepath = save_json(unique, RAW_DIR,  gcs_source="news", hourly=True)
    logger.info(f"已儲存至 {filepath}")

    logger.info("=== 範例資料（前 2 筆）===")
    for a in unique[:2]:
        print(f"  來源           : {a['source_display']}")
        print(f"  標題           : {a['title']}")
        print(f"  發布時間       : {a['published_at']}")
        print(f"  分類           : {a['category']}")
        print(f"  keyword        : {a['keyword']}")
        print(f"  related_stocks : {a['related_stocks']}")
        print(f"  summary 前50字 : {a['summary'][:50]}")
        print()

    return filepath


if __name__ == "__main__":
    run()