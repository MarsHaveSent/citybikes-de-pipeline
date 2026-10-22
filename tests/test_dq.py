import pytest

from citybikes.dq import Check, DataQualityError, evaluate, finish, run_checks

LAG = Check("lag", "bronze", "", warn_at=30, fail_at=120)
COVERAGE = Check("coverage", "bronze", "", warn_at=0.95, fail_at=0.8, lower_is_worse=True)


@pytest.mark.parametrize(
    ("check", "value", "status"),
    [
        (LAG, 10, "ok"),
        (LAG, 30, "ok"),
        (LAG, 31, "warn"),
        (LAG, 121, "fail"),
        (COVERAGE, 0.99, "ok"),
        (COVERAGE, 0.9, "warn"),
        (COVERAGE, 0.5, "fail"),
        (LAG, None, "ok"),
    ],
)
def test_evaluate(check, value, status):
    assert evaluate(check, value) == status


def test_finish_raises_with_failed_checks_only():
    results = run_checks(
        lambda sql, params: {"a": 200, "b": 50}[sql],
        [
            Check("lag_a", "bronze", "a", warn_at=30, fail_at=120),
            Check("lag_b", "bronze", "b", warn_at=30, fail_at=120),
        ],
        {},
    )

    with pytest.raises(DataQualityError) as error:
        finish(results)
    assert "lag_a: 200 (порог 120)" in str(error.value)
    assert "lag_b" not in str(error.value)


def test_finish_counts_statuses():
    results = run_checks(lambda sql, params: 50, [LAG, COVERAGE], {})
    assert finish(results) == {"ok": 1, "warn": 1, "fail": 0}
