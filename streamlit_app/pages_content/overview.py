import streamlit as st
import plotly.graph_objects as go


def render(client):
    st.subheader("今日市場總覽")

    industry_query = """
    SELECT trade_date, industry_code, industry_name, weighted_avg_price, daily_return_pct
    FROM `stockpulse_marts.fact_industry_price`
    WHERE market = 'TWSE'
      AND trade_date = (SELECT MAX(trade_date) FROM `stockpulse_marts.fact_industry_price` WHERE market = 'TWSE')
      AND daily_return_pct IS NOT NULL
      AND industry_name IS NOT NULL
    ORDER BY daily_return_pct DESC
    """
    industry_df = client.query(industry_query).to_dataframe()

    fg_query = """
    SELECT index_date, fear_greed_score, fear_greed_rating
    FROM `stockpulse_marts.fact_fear_greed`
    ORDER BY index_date DESC LIMIT 2
    """
    fg_df = client.query(fg_query).to_dataframe()

    col1, col2, col3 = st.columns(3)
    if len(fg_df) >= 2:
        latest_fg, prev_fg = fg_df.iloc[0], fg_df.iloc[1]
        score_delta = latest_fg["fear_greed_score"] - prev_fg["fear_greed_score"]
        col1.metric(
            "市場情緒指數 (0 fear - 100 greed)",
            f"{latest_fg['fear_greed_score']:.1f} ({latest_fg['fear_greed_rating']})",
            f"{score_delta:+.1f}",
        )
    elif len(fg_df) == 1:
        col1.metric(
            "市場情緒指數 (0 fear - 100 greed)",
            f"{fg_df.iloc[0]['fear_greed_score']:.1f} ({fg_df.iloc[0]['fear_greed_rating']})",
        )

    if not industry_df.empty:
        best, worst = industry_df.iloc[0], industry_df.iloc[-1]
        col2.metric(
            "今日表現最佳產業",
            best["industry_name"],
            f"{best['daily_return_pct']:+.2f}%",
        )
        col3.metric(
            "今日表現最弱產業",
            worst["industry_name"],
            f"{worst['daily_return_pct']:+.2f}%",
        )

    st.divider()

    if not industry_df.empty:
        fig_bar = go.Figure(
            go.Bar(
                x=industry_df["daily_return_pct"],
                y=industry_df["industry_name"],
                orientation="h",
                marker_color=[
                    "red" if v >= 0 else "green"
                    for v in industry_df["daily_return_pct"]
                ],
            )
        )
        fig_bar.update_layout(
            height=700, xaxis_title="漲跌幅 (%)", yaxis=dict(autorange="reversed")
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        st.divider()
        st.subheader("各產業表現")
        sort_option = st.radio(
            "排序方式", ["依表現排序", "依產業代碼排序"], horizontal=True
        )
        display_df = (
            industry_df.sort_values("daily_return_pct", ascending=False)
            if sort_option == "依表現排序"
            else industry_df.sort_values("industry_code")
        )
        cols_per_row = 5
        for i in range(0, len(display_df), cols_per_row):
            row_data = display_df.iloc[i : i + cols_per_row]
            row_cols = st.columns(cols_per_row)
            for col, (_, row) in zip(row_cols, row_data.iterrows()):
                col.metric(
                    row["industry_name"],
                    f"{row['weighted_avg_price']:.2f}",
                    f"{row['daily_return_pct']:+.2f}%",
                )
    else:
        st.warning("目前無產業排行資料")

    st.caption(
        "目前僅顯示上市(TWSE)產業排行,上櫃(TPEx)因歷史資料加權方式限制(詳見 README),暫不納入排行比較"
    )
    st.caption(
        "⚠️ 產業加權指數採成交金額加權,權重可能集中於少數大型股,單日大幅波動不代表產業內多數股票同步表現"
    )
