# CityBikes DE Pipeline

ELT-пайплайн на данных сетей велошеринга: загрузка из CityBikes API в PostgreSQL, обработка на Spark, хранение витрин в HDFS (Delta Lake), аналитика в ClickHouse и Metabase. Весь стек поднимается в Docker.

Помимо разовой загрузки снимка сетей пайплайн регулярно собирает состояние станций, обогащает его погодой и ищет крупные города без велошеринга.

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

## Источник данных

Данные о сетях и станциях предоставляет [CityBikes](https://citybik.es/) через [API v2](https://api.citybik.es/v2/).
