import streamlit as st
import plotly.graph_objects as go


def render(client):
    st.subheader("管線健康度")

    summary_query = """
    SELECT
        dag_id,
        task_id,
        COUNT(*) as total_runs,
        COUNTIF(status = 'success') as success_runs,
        ROUND(COUNTIF(status = 'success') / COUNT(*) * 100, 1) as success_rate_pct,
        ROUND(AVG(duration_seconds), 1) as avg_duration_sec
    FROM `stockpulse_staging.pipeline_run_log`
    WHERE run_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY dag_id, task_id
    ORDER BY dag_id, task_id
    """
    summary_df = client.query(summary_query).to_dataframe()

    if summary_df.empty:
        st.info("目前尚無近 30 天的執行紀錄")
        return

    col1, col2, col3 = st.columns(3)
    overall_success_rate = (
        summary_df["success_runs"].sum() / summary_df["total_runs"].sum() * 100
    )
    col1.metric("整體成功率(近30天)", f"{overall_success_rate:.1f}%")
    col2.metric("總執行次數", int(summary_df["total_runs"].sum()))
    col3.metric("追蹤任務數", summary_df["task_id"].nunique())

    st.divider()

    st.subheader("各任務成功率")
    fig = go.Figure(
        go.Bar(
            x=summary_df["success_rate_pct"],
            y=summary_df["dag_id"] + " / " + summary_df["task_id"],
            orientation="h",
            marker_color=[
                "green" if v >= 95 else ("orange" if v >= 80 else "red")
                for v in summary_df["success_rate_pct"]
            ],
        )
    )
    fig.update_layout(
        height=max(300, len(summary_df) * 30),
        xaxis_title="成功率 (%)",
        xaxis_range=[0, 100],
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("每日執行趨勢")
    trend_query = """
    SELECT DATE(run_timestamp) as run_date, status, COUNT(*) as cnt
    FROM `stockpulse_staging.pipeline_run_log`
    WHERE run_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY run_date, status
    ORDER BY run_date
    """
    trend_df = client.query(trend_query).to_dataframe()

    fig2 = go.Figure()
    for status, color in [("success", "green"), ("failed", "red")]:
        sub = trend_df[trend_df["status"] == status]
        fig2.add_trace(
            go.Bar(x=sub["run_date"], y=sub["cnt"], name=status, marker_color=color)
        )
    fig2.update_layout(
        barmode="stack", height=400, xaxis_title="日期", yaxis_title="執行次數"
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    st.subheader("詳細數據")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
