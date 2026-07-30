import os
from datetime import datetime, timezone
from unittest.mock import patch

import requests

from shared.alerting import send_slack_alert, log_success


class _FakeTaskInstance:
    task_id = "fetch_twse_daily"
    start_date = datetime(2026, 7, 9, 17, 0, tzinfo=timezone.utc)
    end_date = datetime(2026, 7, 9, 17, 1, tzinfo=timezone.utc)
    log_url = "http://airflow/log/123"


class _FakeDag:
    dag_id = "DAG_Taiwan_Market"


def _make_context():
    return {
        "task_instance": _FakeTaskInstance(),
        "dag": _FakeDag(),
        "data_interval_end": datetime(2026, 7, 9, tzinfo=timezone.utc),
        "logical_date": datetime(2026, 7, 9, tzinfo=timezone.utc),
    }


def test_send_slack_alert_skips_post_when_webhook_not_configured():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("SLACK_WEBHOOK_URL", None)
        with patch("shared.alerting.bigquery.Client") as mock_bq, patch(
            "shared.alerting.requests.post"
        ) as mock_post:
            mock_bq.return_value.insert_rows_json.return_value = []
            send_slack_alert(_make_context())

    mock_post.assert_not_called()


def test_send_slack_alert_posts_webhook_with_dag_and_task_info():
    with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://hooks.example/x"}):
        with patch("shared.alerting.bigquery.Client") as mock_bq, patch(
            "shared.alerting.requests.post"
        ) as mock_post:
            mock_bq.return_value.insert_rows_json.return_value = []
            mock_post.return_value.raise_for_status.return_value = None
            send_slack_alert(_make_context())

    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == "https://hooks.example/x"
    assert "DAG_Taiwan_Market" in kwargs["json"]["text"]
    assert "fetch_twse_daily" in kwargs["json"]["text"]


def test_send_slack_alert_swallows_webhook_post_failure():
    """Slack 發送失敗不該讓整個 callback 拋例外(否則會蓋掉原本任務失敗的真正原因)。"""
    with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://hooks.example/x"}):
        with patch("shared.alerting.bigquery.Client") as mock_bq, patch(
            "shared.alerting.requests.post",
            side_effect=requests.exceptions.ConnectionError("boom"),
        ):
            mock_bq.return_value.insert_rows_json.return_value = []
            send_slack_alert(_make_context())  # 不應該 raise


def test_write_run_log_bigquery_failure_does_not_break_alerting():
    """
    _write_run_log 內部包了 try/except:寫 BigQuery 失敗時,不該連帶讓
    send_slack_alert 整個失敗——告警本身比記錄更重要。
    """
    with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://hooks.example/x"}):
        with patch(
            "shared.alerting.bigquery.Client", side_effect=RuntimeError("no creds")
        ), patch("shared.alerting.requests.post") as mock_post:
            mock_post.return_value.raise_for_status.return_value = None
            send_slack_alert(_make_context())  # 不應該 raise

    mock_post.assert_called_once()  # BigQuery 掛了,Slack 告警仍然要照常發送


def test_log_success_never_posts_to_slack():
    """log_success 只記錄成功,絕對不該觸發 Slack 告警(避免每次成功都轟炸)。"""
    with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://hooks.example/x"}):
        with patch("shared.alerting.bigquery.Client") as mock_bq, patch(
            "shared.alerting.requests.post"
        ) as mock_post:
            mock_bq.return_value.insert_rows_json.return_value = []
            log_success(_make_context())

    mock_post.assert_not_called()
    mock_bq.return_value.insert_rows_json.assert_called_once()
    row = mock_bq.return_value.insert_rows_json.call_args[0][1][0]
    assert row["status"] == "success"


if __name__ == "__main__":
    test_send_slack_alert_skips_post_when_webhook_not_configured()
    test_send_slack_alert_posts_webhook_with_dag_and_task_info()
    test_send_slack_alert_swallows_webhook_post_failure()
    test_write_run_log_bigquery_failure_does_not_break_alerting()
    test_log_success_never_posts_to_slack()
    print("✅ 全部測試通過")
