from datetime import date, timedelta
import streamlit as st

RANGE_OPTIONS = ["1D", "5D", "1M", "6M", "YTD", "1Y", "5Y", "ALL"]


def render_date_range_selector() -> date:
    """畫出日期範圍按鈕列,回傳目前選擇對應的起始日期。"""
    if "range_option" not in st.session_state:
        st.session_state.range_option = "1Y"

    cols = st.columns(len(RANGE_OPTIONS))
    for i, opt in enumerate(RANGE_OPTIONS):
        is_selected = st.session_state.range_option == opt
        if cols[i].button(
            opt,
            use_container_width=True,
            type="primary" if is_selected else "secondary",
        ):
            if st.session_state.range_option != opt:
                st.session_state.range_option = opt
                st.rerun()

    today = date.today()
    range_map = {
        "1D": today - timedelta(days=1),
        "5D": today - timedelta(days=5),
        "1M": today - timedelta(days=30),
        "6M": today - timedelta(days=182),
        "YTD": date(today.year, 1, 1),
        "1Y": today - timedelta(days=365),
        "5Y": today - timedelta(days=365 * 5),
        "ALL": date(2024, 7, 1),
    }
    return range_map[st.session_state.range_option]
