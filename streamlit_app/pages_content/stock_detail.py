import pandas as pd
import streamlit as st
from plotly.subplots import make_subplots
import plotly.graph_objects as go


def render(client, start_date_filter):
    st.subheader("個股技術分析面板")

    stock_id = st.text_input("輸入股票代號", value="1101")
    if not stock_id:
        return

    info_query = f"""
    SELECT DISTINCT stock_id, company_name, stock_name, market, industry_name
    FROM `stockpulse_marts.marts_market_obt`
    WHERE stock_id = '{stock_id}'
    LIMIT 1
    """
    info_df = client.query(info_query).to_dataframe()

    if info_df.empty:
        st.warning(f"找不到股票代號 {stock_id} 的基本資料")
        return

    info = info_df.iloc[0]
    market_label = "上市(TWSE)" if info["market"] == "TWSE" else "上櫃(TPEx)"
    st.markdown(f"### {info['company_name']}({info['stock_name']}, {stock_id})")
    st.caption(f"市場別:{market_label} ｜ 產業別:{info['industry_name'] or '未分類'}")

    stock_query = f"""
    SELECT trade_date, open_price, high_price, low_price, close_price,
           trade_volume, ma5, ma20, rsi14
    FROM `stockpulse_marts.fact_stock_daily`
    WHERE stock_id = '{stock_id}' AND trade_date >= '{start_date_filter}'
    ORDER BY trade_date
    """
    df = client.query(stock_query).to_dataframe()

    if df.empty:
        st.warning(f"此區間查無資料")
        return

    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("開盤", f"{latest['open_price']:.2f}")
    col2.metric("最高", f"{latest['high_price']:.2f}")
    col3.metric("最低", f"{latest['low_price']:.2f}")
    col4.metric(
        "收盤",
        f"{latest['close_price']:.2f}",
        f"{latest['close_price'] - prev['close_price']:.2f}",
    )

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("成交量", f"{latest['trade_volume']:,.0f}")
    col6.metric("MA5", f"{latest['ma5']:.2f}" if pd.notna(latest["ma5"]) else "N/A")
    col7.metric("MA20", f"{latest['ma20']:.2f}" if pd.notna(latest["ma20"]) else "N/A")
    if pd.notna(latest["rsi14"]):
        rsi_status = (
            "超買"
            if latest["rsi14"] > 70
            else ("超賣" if latest["rsi14"] < 30 else "中性")
        )
        col8.metric("RSI14", f"{latest['rsi14']:.1f}", rsi_status)
    else:
        col8.metric("RSI14", "N/A")

    st.divider()

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.05,
        subplot_titles=("價格 / K線", "RSI14"),
    )
    fig.add_trace(
        go.Candlestick(
            x=df["trade_date"],
            open=df["open_price"],
            high=df["high_price"],
            low=df["low_price"],
            close=df["close_price"],
            name="K線",
            increasing_line_color="red",
            decreasing_line_color="green",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["trade_date"], y=df["ma5"], name="MA5", line=dict(width=1)),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["trade_date"], y=df["ma20"], name="MA20", line=dict(width=1)),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["trade_date"], y=df["rsi14"], name="RSI14", line=dict(color="purple")
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1) # pyright: ignore[reportArgumentType]
    fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
    fig.update_layout(height=700, xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)

    st.caption(
        "RSI14 為簡化版計算(以簡單移動平均取代標準平滑移動平均),數值與券商軟體可能略有差異,長期趨勢意義一致"
    )
