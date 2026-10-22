from scripts.init_env import missing_lines, render_env

EXAMPLE = """# комментарий
EMPTY_PASSWORD=
AIRFLOW_FERNET_KEY=
PRESET=value
"""


def test_fills_only_empty_values():
    env = dict(
        line.split("=", 1) for line in render_env(EXAMPLE).splitlines() if not line.startswith("#")
    )
    assert env["PRESET"] == "value"
    assert len(env["EMPTY_PASSWORD"]) == 32
    assert env["AIRFLOW_FERNET_KEY"].endswith("=")


def test_keeps_comments():
    assert render_env(EXAMPLE).startswith("# комментарий\n")


def test_missing_lines_adds_only_new_keys():
    added = missing_lines(EXAMPLE, "EMPTY_PASSWORD=old\nPRESET=other\n")
    assert [line.partition("=")[0] for line in added] == ["AIRFLOW_FERNET_KEY"]


def test_manual_keys_stay_empty():
    env = render_env("SMTP_PASSWORD=\nSMTP_HOST=smtp.mail.ru\n")
    assert env == "SMTP_PASSWORD=\nSMTP_HOST=smtp.mail.ru\n"
