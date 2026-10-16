# CityBikes DE Pipeline

[![CI](https://github.com/MarsHaveSent/citybikes-de-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/MarsHaveSent/citybikes-de-pipeline/actions/workflows/ci.yml)

ELT-пайплайн на данных сетей велошеринга: загрузка из CityBikes API в PostgreSQL, обработка на Spark, хранение витрин в HDFS (Delta Lake), аналитика в ClickHouse и Metabase. Весь стек поднимается в Docker.

Пайплайн регулярно собирает каталог сетей и состояние станций с учётом лимита API, обогащает данные погодой и ищет крупные города без велошеринга.

> Проект в разработке.

## Стек

Python 3.12, Apache Airflow 3, PostgreSQL, Apache Spark, HDFS, Delta Lake, ClickHouse, Metabase, Docker, uv, GitHub Actions.

## Запуск локально

Нужны Docker и Python 3.

```bash
python scripts/init_env.py   # создаёт .env со случайными паролями и ключами
docker compose up -d
```

- Airflow: http://localhost:8080, логин и пароль — `AIRFLOW_ADMIN_USER` и `AIRFLOW_ADMIN_PASSWORD` из `.env`
- PostgreSQL: `localhost:15432`, БД `citybikes`, пользователь `citybikes`
- Spark Master UI: http://localhost:8081, Spark Worker UI: http://localhost:8082
- HDFS NameNode UI: http://localhost:9870

## Тесты

```bash
uv sync
uv run pytest
```

Тесты из `tests/integration` работают с PostgreSQL, без переменной `TEST_DATABASE_URL` пропускаются. Каждый тест создаёт себе отдельную схему и удаляет её после себя, поэтому подойдёт любая база, например временный контейнер:

```bash
docker run -d --rm --name pg-test -e POSTGRES_PASSWORD=test -p 127.0.0.1:15499:5432 postgres:17
TEST_DATABASE_URL=postgresql://postgres:test@127.0.0.1:15499/postgres uv run pytest
```

В CI на каждый пуш запускаются pre-commit, все тесты с PostgreSQL и проверка DAGов в образе Airflow (`scripts/check_dags.py`).

## Деплой

Сервер обновляется при пуше в `main`, если прошли все проверки CI:

1. Образы Airflow, Spark и Hadoop собираются в GitHub Actions и публикуются в GHCR. Тег образа — хеш его каталога `docker/<образ>` в git ([deploy/image-tag.sh](deploy/image-tag.sh)), поэтому образ пересобирается, только когда меняются его файлы.
2. По SSH на сервере запускается [deploy/deploy.sh](deploy/deploy.sh): код переключается на этот коммит, образы — на теги, вычисленные из него же, стек перезапускается, скрипт ждёт, пока сервисы пройдут healthcheck. DAG'и, код и SQL смонтированы в контейнеры и обновляются вместе с git, без пересборки образов.

На сервере к `docker-compose.yml` подключается [deploy/compose.prod.yml](deploy/compose.prod.yml): образы из GHCR вместо локальной сборки и Caddy, который получает сертификат Let's Encrypt и открывает Airflow по адресу `https://<домен>/airflow/`. Остальные сервисы наружу не публикуются. Первичная настройка чистого сервера — [deploy/bootstrap.sh](deploy/bootstrap.sh).

## Источник данных

Данные о сетях и станциях предоставляет [CityBikes](https://citybik.es/) через [API v2](https://api.citybik.es/v2/).

Погодные данные: [Open-Meteo](https://open-meteo.com/), лицензия [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
