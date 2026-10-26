#!/usr/bin/env bash
# Создаёт пользователей и базы сервисов, которых ещё нет;
# Запускается сервисом postgres-init при каждом старте стека
# Пароли существующих пользователей не меняются
set -euo pipefail

create() {
    psql -v ON_ERROR_STOP=1 --dbname postgres -v name="$1" -v pw="$2" <<'EOSQL'
SELECT format('CREATE USER %I WITH PASSWORD %L', :'name', :'pw')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'name') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'name', :'name')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'name') \gexec
EOSQL
}

create airflow "$AIRFLOW_DB_PASSWORD"
create citybikes "$CITYBIKES_DB_PASSWORD"
create metabase "$METABASE_DB_PASSWORD"
