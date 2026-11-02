#!/usr/bin/env bash
# Фаервол сервера. Запуск от root после bootstrap.sh, повторный запуск безопасен:
#   ssh root@<host> 'bash -s' < deploy/firewall.sh
set -euo pipefail

echo "== ufw: порты самого хоста"
# Правило панели хостинга от шаблона ОС, панель не установлена
ufw delete allow ispmanager >/dev/null 2>&1 || true
ufw allow 22/tcp >/dev/null
ufw allow 80/tcp >/dev/null
ufw allow 443/tcp >/dev/null

echo "== Docker: Postgres 6432 только для Yandex DataLens"
# Порты контейнеров Docker открывает в обход ufw: такие пакеты идут через цепочку FORWARD,
# где Docker первым делом проверяет DOCKER-USER. Порт сравнивается с исходным (ctorigdstport):
# после перенаправления в контейнер пакет идёт уже на 5432.
# Адреса DataLens: https://yandex.cloud/ru/docs/datalens/operations/connection/create-postgresql,
# те же диапазоны в docker/postgres/pg_hba.conf.
# Блок живёт в after.rules и применяется при каждом старте ufw; при перезагрузке ufw
# объявление цепочки очищает её, правила не дублируются
RULES=/etc/ufw/after.rules
BEGIN="# BEGIN citybikes DOCKER-USER"
END="# END citybikes DOCKER-USER"
sed -i "/^$BEGIN\$/,/^$END\$/d" "$RULES"
{
    echo "$BEGIN"
    echo "*filter"
    echo ":DOCKER-USER - [0:0]"
    for cidr in 178.154.242.128/26 178.154.242.192/27 130.193.60.0/27; do
        echo "-A DOCKER-USER -p tcp -m conntrack --ctorigdstport 6432 --ctdir ORIGINAL -s $cidr -j RETURN"
    done
    echo "-A DOCKER-USER -p tcp -m conntrack --ctorigdstport 6432 --ctdir ORIGINAL -j DROP"
    echo "-A DOCKER-USER -j RETURN"
    echo "COMMIT"
    echo "$END"
} >> "$RULES"

ufw --force enable >/dev/null
ufw reload >/dev/null

echo "== Итог"
ufw status | grep ALLOW
iptables -S DOCKER-USER
