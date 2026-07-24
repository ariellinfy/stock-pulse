import streamlit as st
from plotly.subplots import make_subplots
import plotly.graph_objects as go


def render(client, start_date_filter):
    st.subheader("產業表現 vs 總經情緒對照")

    available_industries = client.query(
        "SELECT DISTINCT industry_code, industry_name FROM `stockpulse_marts.dim_industry` WHERE industry_name IS NOT NULL ORDER BY industry_code"
    ).to_dataframe()

    if "selected_industry" not in st.session_state:
        st.session_state.selected_industry = (
            "半導體業" if "半導體業" in available_industries["industry_name"].values
            else available_industries.iloc[0]["industry_name"]
        )

    industries_list = available_industries["industry_name"].tolist()
    cols_per_row = 7
    for i in range(0, len(industries_list), cols_per_row):
        row_names = industries_list[i:i + cols_per_row]
        row_cols = st.columns(cols_per_row)
        for col, name in zip(row_cols, row_names):
            is_selected = st.session_state.selected_industry == name
            if col.button(name, use_container_width=True, type="primary" if is_selected else "secondary", key=f"industry_btn_{name}"):
                if st.session_state.selected_industry != name:
                    st.session_state.selected_industry = name
                    st.rerun()

    selected_industry = st.session_state.selected_industry
    st.caption(f"目前選擇: {selected_industry}")

    compare_query = f"""
    SELECT ip.trade_date, ip.weighted_avg_price, fg.fear_greed_score
    FROM `stockpulse_marts.fact_industry_price` ip
    LEFT JOIN `stockpulse_marts.fact_fear_greed` fg ON ip.trade_date = fg.index_date
    WHERE ip.industry_name = '{selected_industry}'
      AND ip.market = 'TWSE'
      AND ip.trade_date >= '{start_date_filter}'
    ORDER BY ip.trade_date
    """
    compare_df = client.query(compare_query).to_dataframe()

    if not compare_df.empty:
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_trace(go.Scatter(x=compare_df["trade_date"], y=compare_df["weighted_avg_price"], name=f"{selected_industry}指數", line=dict(color="blue")), secondary_y=False)
        fig2.add_trace(go.Scatter(x=compare_df["trade_date"], y=compare_df["fear_greed_score"], name="Fear & Greed", line=dict(color="orange")), secondary_y=True)
        fig2.update_yaxes(title_text=f"{selected_industry}加權指數", secondary_y=False)
        fig2.update_yaxes(title_text="Fear & Greed 分數", secondary_y=True)
        fig2.update_layout(height=500)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("此區間查無資料")