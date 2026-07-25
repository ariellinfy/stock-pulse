import os
import requests
from datetime import datetime, timezone
from google.cloud import bigquery


def _write_run_log(context, status: str):
    """
    把這次任務執行的結果,寫進 pipeline_run_log。
    row_count 目前無法從 context 直接取得(那是任務函式內部才知道的細節),
    先留空,若未來需要更精確的筆數記錄,可透過 XCom 傳遞。
    """
    try:
        os.environ.setdefault(
            "GOOGLE_APPLICATION_CREDENTIALS", os.environ.get("GCP_SA_KEY_PATH", "")
        )
        client = bigquery.Client(project=os.environ.get("GCP_PROJECT_ID"))

        ti = context["task_instance"]

        duration = None
        if ti.start_date and ti.end_date:
            duration = (ti.end_date - ti.start_date).total_seconds()

        row = {
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "dag_id": context["dag"].dag_id,
            "task_id": ti.task_id,
            "target_date": (
                context.get("data_interval_end").date().isoformat()
                if context.get("data_interval_end")
                else None
            ),
            "status": status,
            "row_count": None,
            "duration_seconds": duration,
        }

        errors = client.insert_rows_json("stockpulse_staging.pipeline_run_log", [row])
        if errors:
            print(f"⚠️ pipeline_run_log 寫入失敗: {errors}")
    except Exception as e:
        # 記錄本身失敗,不該影響主流程或告警,只印出來
        print(f"⚠️ 無法寫入 pipeline_run_log: {e}")


def send_slack_alert(context):
    """Airflow on_failure_callback:記錄失敗 + 發送 Slack 告警。"""
    _write_run_log(context, status="failed")

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("⚠️ SLACK_WEBHOOK_URL 未設定,略過告警發送")
        return

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    logical_date = context.get("logical_date", context.get("execution_date"))
    log_url = context["task_instance"].log_url

    message = {
        "text": (
            f":red_circle: *Airflow 任務失敗*\n"
            f"*DAG*: `{dag_id}`\n"
            f"*Task*: `{task_id}`\n"
            f"*時間*: {logical_date}\n"
            f"*Log*: {log_url}"
        )
    }

    try:
        resp = requests.post(webhook_url, json=message, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Slack 告警發送失敗: {e}")


def log_success(context):
    """Airflow on_success_callback:只記錄成功,不發告警(避免每次成功都轟炸 Slack)。"""
    _write_run_log(context, status="success")
