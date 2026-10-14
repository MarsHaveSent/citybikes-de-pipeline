#!/usr/bin/env bash
# Первичная настройка чистого Ubuntu 24.04 под проект. Запуск от root:
#   ssh root@<host> 'bash -s' < deploy/bootstrap.sh
# Повторный запуск безопасен.
set -euo pipefail

DEPLOY_USER=deploy

echo "== SSH: вход только по ключу"
# В sshd побеждает первое встреченное значение, а файлы из sshd_config.d читаются по алфавиту:
# 00-* перекрывает настройки хостера, не изменяя их файл
cat > /etc/ssh/sshd_config.d/00-hardening.conf <<'EOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
EOF
sshd -t
systemctl reload ssh

echo "== Фаервол"
# Порты контейнеров Docker открывает в обход ufw, поэтому внутренние сервисы
# в compose привязаны к 127.0.0.1. ufw защищает то, что слушает сам хост.
# Правило панели хостинга от шаблона ОС, панель не установлена
ufw delete allow ispmanager >/dev/null 2>&1 || true
ufw allow 22/tcp >/dev/null
ufw allow 80/tcp >/dev/null
ufw allow 443/tcp >/dev/null
ufw --force enable >/dev/null

echo "== Docker"
if ! command -v docker >/dev/null; then
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc]" \
        "https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        > /etc/apt/sources.list.d/docker.list
    apt-get update -q
    apt-get install -y -q docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
# Ротация json-логов
DAEMON_JSON='{"log-driver": "json-file", "log-opts": {"max-size": "20m", "max-file": "3"}}'
if [ "$(cat /etc/docker/daemon.json 2>/dev/null)" != "$DAEMON_JSON" ]; then
    echo "$DAEMON_JSON" > /etc/docker/daemon.json
    systemctl restart docker
fi

echo "== Пользователь $DEPLOY_USER"
id "$DEPLOY_USER" >/dev/null 2>&1 || useradd --create-home --shell /bin/bash "$DEPLOY_USER"
usermod -aG docker "$DEPLOY_USER"
install -d -m 700 -o "$DEPLOY_USER" -g "$DEPLOY_USER" "/home/$DEPLOY_USER/.ssh"
install -m 600 -o "$DEPLOY_USER" -g "$DEPLOY_USER" /root/.ssh/authorized_keys \
    "/home/$DEPLOY_USER/.ssh/authorized_keys"

echo "== Swap"
# zram от хостера сжимает страницы в RAM; файл на диске с низким приоритетом -
# последний запас перед OOM на пиках Spark-джоб
if [ ! -f /swapfile ]; then
    fallocate -l 4G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile >/dev/null
    swapon --priority 10 /swapfile
    echo "/swapfile none swap sw,pri=10 0 0" >> /etc/fstab
fi

echo "== Итог"
sshd -T | grep -E "^(passwordauthentication|permitrootlogin) "
ufw status | grep ALLOW
docker --version
docker compose version
id "$DEPLOY_USER"
swapon --show
