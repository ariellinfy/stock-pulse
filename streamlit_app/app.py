import streamlit as st

from db import get_bq_client
from filters import render_date_range_selector
from pages_content import overview, industry_sentiment, stock_detail

st.set_page_config(page_title="stock-pulse", layout="wide")

client = get_bq_client()

st.title("📈 Stock Pulse 全台股市場儀表板")

last_updated_query = """
SELECT MAX(trade_date) as latest_stock_date
FROM `stockpulse_marts.fact_stock_daily`
"""
latest_date = client.query(last_updated_query).to_dataframe().iloc[0]["latest_stock_date"]

st.caption(f"📅 資料最後更新至:{latest_date}(每日台北時間 17:00 自動更新)")

start_date_filter = render_date_range_selector()

st.divider()

tab1, tab2, tab3 = st.tabs(["📊 今日市場總覽", "🏭 產業 vs 情緒", "🔍 個股技術分析"])

with tab1:
    overview.render(client)

with tab2:
    industry_sentiment.render(client, start_date_filter)

with tab3:
    stock_detail.render(client, start_date_filter)