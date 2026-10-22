from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from citybikes import monitoring

START = datetime(2026, 10, 21, 10, 0, tzinfo=UTC)
END = datetime(2026, 10, 21, 10, 1, 30, tzinfo=UTC)


class PythonOperator:
    pass


class EmptyOperator:
    pass


def ti(task=None):
    return SimpleNamespace(
        dag_id="citybikes_weather",
        task_id="load_weather",
        run_id="manual__1",
        try_number=2,
        start_date=START,
        task=task or PythonOperator(),
        xcom_pull=lambda task_ids: {"hours": 480},
    )


@pytest.fixture
def spy(monkeypatch):
    """Подменяет журнал и почту; last - последнее итоговое состояние задачи в журнале."""
    calls = SimpleNamespace(recorded=[], sent=[], last=None)
    monkeypatch.setattr(monitoring, "_record", calls.recorded.append)
    monkeypatch.setattr(monitoring, "_last_final_state", lambda dag_id, task_id: calls.last)
    monkeypatch.setattr(monitoring.alerts, "send_alert", lambda s, b: calls.sent.append((s, b)))
    return calls


def fail(error="нет координат"):
    monitoring.on_failure({"ti": ti(), "exception": ValueError(error)})


def test_row_keeps_dict_metrics_and_duration():
    row = monitoring.task_run_row(ti(), "success", {"hours": 480}, None, END)
    assert row[:5] == ("citybikes_weather", "load_weather", "manual__1", 2, "success")
    assert row[7] == 90
    assert row[8].obj == {"hours": 480}


def test_row_ignores_non_dict_result():
    row = monitoring.task_run_row(ti(), "success", [[""]], None, END)
    assert row[8] is None


def test_failure_records_error_and_sends_alert(spy):
    fail()

    assert spy.recorded[0][4] == "failed"
    assert spy.recorded[0][9] == "ValueError: нет координат"
    assert spy.sent[0][0] == "citybikes_weather.load_weather упала"
    assert "нет координат" in spy.sent[0][1]


def test_alert_is_sent_when_journal_is_unavailable(spy, monkeypatch):
    def unavailable(*args):
        raise ConnectionError("postgres недоступен")

    monkeypatch.setattr(monitoring, "_last_final_state", unavailable)
    monkeypatch.setattr(monitoring, "_record", unavailable)

    fail()

    assert len(spy.sent) == 1


def test_failure_is_recorded_when_mail_is_unavailable(spy, monkeypatch):
    def smtp_down(subject, body):
        raise TimeoutError("smtp")

    monkeypatch.setattr(monitoring.alerts, "send_alert", smtp_down)

    fail()

    assert spy.recorded[0][4] == "failed"


def test_repeated_failure_is_not_alerted_again(spy):
    spy.last = ("failed", datetime.now(UTC) - timedelta(hours=1))

    fail()

    assert spy.sent == []
    assert spy.recorded[0][4] == "failed"


def test_failure_is_reminded_after_a_day(spy):
    spy.last = ("failed", datetime.now(UTC) - timedelta(hours=25))

    fail()

    assert len(spy.sent) == 1


def test_recovery_is_alerted_once(spy):
    spy.last = ("failed", datetime.now(UTC) - timedelta(hours=1))
    monitoring.on_success({"ti": ti()})
    spy.last = ("success", datetime.now(UTC))
    monitoring.on_success({"ti": ti()})

    assert [subject for subject, _ in spy.sent] == ["citybikes_weather.load_weather снова работает"]
    assert spy.recorded[0][8].obj == {"hours": 480}


def test_retry_errors_do_not_propagate(monkeypatch):
    def broken(row):
        raise ConnectionError("postgres недоступен")

    monkeypatch.setattr(monitoring, "_record", broken)
    monitoring.on_retry({"ti": ti(), "exception": None})


def test_empty_operators_are_skipped(spy):
    monitoring.on_failure({"ti": ti(EmptyOperator()), "exception": None})

    assert spy.recorded == [] and spy.sent == []
