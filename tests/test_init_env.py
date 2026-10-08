from scripts.init_env import render_env

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
