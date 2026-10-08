#!/usr/bin/env bash
# Entrypoint образа postgres запускает скрипт только при первой инициализации тома.
# Если поменять пароли в .env позже, в БД они не изменятся: нужен ALTER USER или пересоздание тома.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -v airflow_pw="$AIRFLOW_DB_PASSWORD" \
    -v citybikes_pw="$CITYBIKES_DB_PASSWORD" <<'EOSQL'
CREATE USER airflow WITH PASSWORD :'airflow_pw';
CREATE DATABASE airflow OWNER airflow;

CREATE USER citybikes WITH PASSWORD :'citybikes_pw';
CREATE DATABASE citybikes OWNER citybikes;
EOSQL
