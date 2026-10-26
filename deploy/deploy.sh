#!/usr/bin/env bash
# Обновление сервера до коммита, прошедшего CI. Запускается из GitHub Actions:
#   ssh deploy@<host> 'bash -s' -- <sha> < deploy/deploy.sh
# В .env на сервере заданы COMPOSE_FILE=docker-compose.yml:deploy/compose.prod.yml и SITE_DOMAIN.
set -euo pipefail

SHA=$1
cd ~/citybikes

set_env() {
    if grep -q "^$1=" .env; then
        sed -i "s|^$1=.*|$1=$2|" .env
    else
        echo "$1=$2" >> .env
    fi
}

git fetch --quiet origin main
git reset --quiet --hard "$SHA"

# Пароли для сервисов, добавленных в .env.example после прошлого деплоя; старые ключи не меняются
python3 scripts/init_env.py

# DAG'и, код и SQL смонтированы в контейнеры из этого каталога и обновляются вместе с git.
# Образы меняются, только если менялись их файлы в docker/
for name in airflow spark hadoop; do
    set_env "${name^^}_IMAGE_TAG" "$(bash deploy/image-tag.sh "$name")"
done

docker compose pull --quiet
# Сначала поднимаются все сервисы, включая одноразовые *-init: от hdfs-init никто не зависит,
# и без этого шага его контейнер остался бы на старом образе.
docker compose up -d --remove-orphans
# --wait считает ошибкой штатное завершение *-init, поэтому ждём только долгоживущие сервисы
mapfile -t services < <(docker compose config --services | grep -v -- '-init$')
docker compose up -d --wait --wait-timeout 600 "${services[@]}"
python3 scripts/metabase_setup.py
# Удаляет все образы которые не использует ни один контейнер.
docker image prune --all --force >/dev/null

git log -1 --format='Задеплоен %h: %s'
