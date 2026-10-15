"""Проверка DAG для CI: запускается внутри образа Airflow с теми же томами, что в compose.

Падает, если DAG не импортируется или файл шаблона (.sql) не найден
"""

import sys

from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = "/opt/airflow/dags"


def unresolved_templates(bag: DagBag) -> list[str]:
    problems = []
    for dag in bag.dags.values():
        for task in dag.tasks:
            for field in task.template_fields:
                value = getattr(task, field, None)
                if isinstance(value, str) and value.endswith(tuple(task.template_ext)):
                    problems.append(f"{dag.dag_id}.{task.task_id}: не найден шаблон {value}")
    return problems


def main() -> int:
    bag = DagBag(dag_folder=DAGS_FOLDER)
    problems = [f"{path}:\n{error}" for path, error in bag.import_errors.items()]
    problems += unresolved_templates(bag)
    if not bag.dags:
        problems.append(f"В {DAGS_FOLDER} не найдено ни одного DAG")

    for dag_id in sorted(bag.dags):
        print(f"ok  {dag_id}")
    for problem in problems:
        print(f"ERR {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
